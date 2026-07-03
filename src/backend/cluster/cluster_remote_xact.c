/*-------------------------------------------------------------------------
 *
 * cluster_remote_xact.c
 *	  Per-origin materialized transaction outcomes (spec-4.5a G5, D10).
 *
 *	  See cluster_remote_xact.h for the design contract.  Mechanically
 *	  this is a small SLRU in the commit_ts mould: fixed-width entries,
 *	  xid-derived page numbers, one control LWLock from a named tranche,
 *	  buffer locks from a dynamically allocated tranche whose id lives in
 *	  shmem so EXEC_BACKEND children register the same id.
 *
 *	  Writers: ONLY the startup process, inside merged replay (single
 *	  threaded).  Readers: any backend after recovery, through
 *	  cluster_remote_commit_outcome.  A missing segment/page reads as
 *	  INDOUBT -- the fail-closed default for "this node never
 *	  materialized that origin's outcome".
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_remote_xact.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Spec: spec-4.5a-shared-storage-data-backend.md (FROZEN v1.0, D10)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/slru.h"
#include "access/transam.h"
#include "access/xact.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "storage/sync.h"
#include "utils/elog.h"

#include "cluster/cluster_elog.h"
#include "cluster/cluster_epoch.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_remote_xact.h"
#include "cluster/cluster_scn.h"				/* recovery_replay_observe (G6) */
#include "cluster/cluster_terminal_authority.h" /* spec-6.2 authority substrate */
#include "cluster/cluster_tt_durable.h"			/* durable wrap cross-check (G6 P1 #2) */
#include "cluster/storage/cluster_undo_xlog.h"	/* redo_stamp_slot (G6) */

#ifdef USE_PGRAC_CLUSTER

/*
 * Entry layout: 16 bytes (8B SCN + 1B status + 7B reserved/zero), 512
 * entries per BLCKSZ page.  Page space: origin (7 bits, CLUSTER_MAX_NODES
 * = 128) << 23 | xid-page (2^32 xids / 512 = 2^23 pages) -- 30 bits, a
 * positive int pageno.
 */
typedef struct ClusterRemoteXactEntry {
	SCN commit_scn;	  /* valid iff status == COMMITTED */
	uint8 status;	  /* ClusterRemoteXactOutcome value */
	uint8 wrap_valid; /* spec-4.5a G6 (P1 #2): 1 iff `wrap` carries the TT slot
					   * reuse generation from the commit record's TT delta */
	uint16 wrap;	  /* TT wrap generation of the committing xact (D4.1 tt_commit.wrap) */
	uint8 _zero[4];	  /* reserved; zero (entry epoch bits live here for a
					   * future store that must span full xid wraparound) */
} ClusterRemoteXactEntry;

StaticAssertDecl(sizeof(ClusterRemoteXactEntry) == CLUSTER_REMOTE_XACT_ENTRY_BYTES,
				 "remote-xact entry must be 16 bytes");

static SlruCtlData ClusterRemoteXactCtlData;
#define ClusterRemoteXactCtl (&ClusterRemoteXactCtlData)

#define CLUSTER_REMOTE_XACT_BUFFERS 8

typedef struct ClusterRemoteXactShared {
	int buffer_tranche_id; /* allocated once by the postmaster */
	pg_atomic_uint64 diverted_commit_count;
	pg_atomic_uint64 diverted_abort_count;
	pg_atomic_uint64 outcome_indoubt_count;
} ClusterRemoteXactShared;

static ClusterRemoteXactShared *RemoteXactShared = NULL;

/*
 * Online-writer scope depth (spec-4.11 3b-2, R14).  Process-local: the online
 * thread-recovery orchestrator brackets its visibility apply with
 * cluster_remote_xact_online_writer_push/pop so that cluster_remote_xact_set's
 * historically startup-only writer assert admits the episode-fenced
 * recovery-apply bgworker.  Process-local is correct (the assert asks "is THIS
 * process a legitimate writer right now"); a FATAL/PANIC tears the process down
 * so a leaked depth cannot outlive it.
 */
static int remote_xact_online_writer_depth_v = 0;

void
cluster_remote_xact_online_writer_push(void)
{
	remote_xact_online_writer_depth_v++;
}

void
cluster_remote_xact_online_writer_pop(void)
{
	Assert(remote_xact_online_writer_depth_v > 0);
	if (remote_xact_online_writer_depth_v > 0)
		remote_xact_online_writer_depth_v--;
}

int
cluster_remote_xact_online_writer_depth(void)
{
	return remote_xact_online_writer_depth_v;
}

static bool
remote_xact_page_precedes(int page1, int page2)
{
	/* No wraparound truncation in this store; plain ordering suffices. */
	return page1 < page2;
}

Size
cluster_remote_xact_shmem_size(void)
{
	return add_size(SimpleLruShmemSize(CLUSTER_REMOTE_XACT_BUFFERS, 0),
					MAXALIGN(sizeof(ClusterRemoteXactShared)));
}

void
cluster_remote_xact_shmem_request(void)
{
	RequestNamedLWLockTranche("ClusterRemoteXactSLRU", 1);
}

void
cluster_remote_xact_shmem_init(void)
{
	bool found;
	LWLock *ctl_lock = &(GetNamedLWLockTranche("ClusterRemoteXactSLRU"))[0].lock;

	RemoteXactShared = (ClusterRemoteXactShared *)ShmemInitStruct(
		"ClusterRemoteXact shared", sizeof(ClusterRemoteXactShared), &found);
	if (!IsUnderPostmaster) {
		Assert(!found);
		RemoteXactShared->buffer_tranche_id = LWLockNewTrancheId();
		pg_atomic_init_u64(&RemoteXactShared->diverted_commit_count, 0);
		pg_atomic_init_u64(&RemoteXactShared->diverted_abort_count, 0);
		pg_atomic_init_u64(&RemoteXactShared->outcome_indoubt_count, 0);

		/*
		 * Create the SLRU directory (postmaster-once).  Unlike pg_xact, this
		 * custom per-origin store is not laid down by initdb, so without this
		 * the first segment write fails with ENOENT on the directory itself
		 * (SlruInternalWritePage O_CREAT creates the FILE, not the DIR).
		 */
		if (MakePGDirectory("pg_xact_remote") < 0 && errno != EEXIST)
			ereport(FATAL, (errcode_for_file_access(),
							errmsg("could not create directory \"pg_xact_remote\": %m")));
	} else {
		Assert(found);
	}
	LWLockRegisterTranche(RemoteXactShared->buffer_tranche_id, "ClusterRemoteXactBuffer");

	ClusterRemoteXactCtl->PagePrecedes = remote_xact_page_precedes;
	SimpleLruInit(ClusterRemoteXactCtl, "ClusterRemoteXact", CLUSTER_REMOTE_XACT_BUFFERS, 0,
				  ctl_lock, "pg_xact_remote", RemoteXactShared->buffer_tranche_id,
				  SYNC_HANDLER_NONE);
}

/*
 * Fetch (read-locking) the page slot for (origin, xid), materializing a
 * zero page when absent and `create` is true.  Returns slotno with the
 * control lock HELD (exclusive); caller releases.
 */
static int
remote_xact_open_page(int origin_node, TransactionId xid, bool create)
{
	int pageno = cluster_remote_xact_pageno(origin_node, xid);
	SlruShared shared = ClusterRemoteXactCtl->shared;
	int slotno;

	LWLockAcquire(shared->ControlLock, LW_EXCLUSIVE);

	/*
	 * Buffer first.  During merged replay a zeroed page lives ONLY in the
	 * SLRU buffer pool -- the file materializes at the end-of-replay flush
	 * (cluster_remote_xact_flush).  Probing the PHYSICAL file to decide
	 * "zero vs read" would re-zero the page on EVERY set(), wiping all
	 * previously recorded outcomes on it but the last one (every earlier
	 * peer commit then reads INDOUBT -> 53R97).  A buffered page also
	 * satisfies the probe-only read path.
	 */
	for (slotno = 0; slotno < shared->num_slots; slotno++) {
		if (shared->page_number[slotno] == pageno && shared->page_status[slotno] != SLRU_PAGE_EMPTY)
			return SimpleLruReadPage(ClusterRemoteXactCtl, pageno, true, xid);
	}

	if (!SimpleLruDoesPhysicalPageExist(ClusterRemoteXactCtl, pageno)) {
		if (!create) {
			/* Probe-only miss: keep the lock-held contract, caller maps to
			 * INDOUBT after releasing. */
			return -1;
		}
		return SimpleLruZeroPage(ClusterRemoteXactCtl, pageno);
	}
	return SimpleLruReadPage(ClusterRemoteXactCtl, pageno, true, xid);
}

/*
 * cluster_remote_xact_set -- record one outcome (startup process only).
 */
static void
cluster_remote_xact_set(int origin_node, TransactionId xid, ClusterRemoteXactOutcome outcome,
						SCN commit_scn, bool wrap_valid, uint16 wrap)
{
	int slotno;
	ClusterRemoteXactEntry *entry;

	/*
	 * R14 (spec-4.11 3b-2): historically startup-only.  Online thread recovery
	 * adds the recovery-apply bgworker as a legitimate writer, but ONLY inside
	 * an episode-fenced online-writer scope -- a stray non-startup writer
	 * outside that scope still fails closed (cluster_remote_xact_writer_allowed
	 * pins the boundary).  !IsUnderPostmaster (bootstrap/standalone) is admitted
	 * as before.
	 */
	Assert(!IsUnderPostmaster
		   || cluster_remote_xact_writer_allowed(AmStartupProcess(),
												 remote_xact_online_writer_depth_v));
	Assert(origin_node >= 0 && origin_node < (1 << 7));

	slotno = remote_xact_open_page(origin_node, xid, true);
	entry = (ClusterRemoteXactEntry *)(ClusterRemoteXactCtl->shared->page_buffer[slotno]
									   + (Size)cluster_remote_xact_entryno(xid)
											 * sizeof(ClusterRemoteXactEntry));
	entry->commit_scn = (outcome == CLUSTER_REMOTE_XACT_COMMITTED) ? commit_scn : InvalidScn;
	entry->status = (uint8)outcome;
	entry->wrap_valid = (outcome == CLUSTER_REMOTE_XACT_COMMITTED && wrap_valid) ? 1 : 0;
	entry->wrap = (uint16)((outcome == CLUSTER_REMOTE_XACT_COMMITTED && wrap_valid) ? wrap : 0);
	ClusterRemoteXactCtl->shared->page_dirty[slotno] = true;
	LWLockRelease(ClusterRemoteXactCtl->shared->ControlLock);
}

ClusterRemoteXactOutcome
cluster_remote_commit_outcome(int origin_node, TransactionId xid, SCN *commit_scn)
{
	return cluster_remote_commit_outcome_ex(origin_node, xid, commit_scn, NULL, NULL);
}

/*
 * Legacy durable-check path used when spec-6.2 terminal authority is disabled.
 * This preserves the Stage 5 behavior for existing callers: COMMITTED must
 * still be backed by an exact durable TT slot match, but membership/retention
 * authority is not required because the aggressive 6.2 transfer paths are off.
 */
static ClusterRemoteXactOutcome
cluster_remote_outcome_durable_checked_legacy(int origin_node, TransactionId xid, SCN *out_scn)
{
	SCN outcome_scn;
	SCN durable_scn;
	uint16 outcome_wrap;
	bool outcome_wrap_valid;
	ClusterRemoteXactOutcome oc;

	if (out_scn != NULL)
		*out_scn = InvalidScn;

	oc = cluster_remote_commit_outcome_ex(origin_node, xid, &outcome_scn, &outcome_wrap,
										  &outcome_wrap_valid);
	if (oc != CLUSTER_REMOTE_XACT_COMMITTED) {
		if (oc == CLUSTER_REMOTE_XACT_ABORTED)
			return CLUSTER_REMOTE_XACT_ABORTED;
		return CLUSTER_REMOTE_XACT_INDOUBT;
	}

	if (!outcome_wrap_valid
		|| cluster_tt_slot_durable_resolve_by_xid_origin(origin_node, xid, (uint32)outcome_wrap,
														 &durable_scn, NULL, NULL, NULL)
			   != CLUSTER_TT_DURABLE_RESOLVED_SCN
		|| durable_scn != outcome_scn) {
		if (RemoteXactShared != NULL)
			pg_atomic_fetch_add_u64(&RemoteXactShared->outcome_indoubt_count, 1);
		return CLUSTER_REMOTE_XACT_INDOUBT;
	}

	if (out_scn != NULL)
		*out_scn = outcome_scn;
	return CLUSTER_REMOTE_XACT_COMMITTED;
}

/*
 * cluster_remote_outcome_terminal_authorized -- spec-6.2 Smart Fusion terminal
 * authority read.  This wraps the existing materialized outcome + durable-TT
 * exact-match proof in an explicit fail-closed evidence decision:
 *
 *   - membership epoch must be known and unchanged;
 *   - the origin-owned store partition must match the claimed origin;
 *   - COMMITTED must carry a valid commit SCN and independent durable-TT proof;
 *   - optional retention decisions must pass an explicit retention proof.
 *
 * Any missing leg returns INDOUBT and bumps the spec-6.2 fail-closed counters.
 * It never falls back to native local CLOG or UNKNOWN-visible semantics.
 */
ClusterRemoteXactOutcome
cluster_remote_outcome_terminal_authorized(int origin_node, TransactionId xid,
										   uint64 observed_epoch, uint64 current_epoch,
										   bool retention_required, bool retention_proven,
										   SCN *out_scn)
{
	SCN outcome_scn;
	SCN durable_scn;
	uint16 outcome_wrap;
	bool outcome_wrap_valid;
	ClusterRemoteXactOutcome oc;
	ClusterTerminalAuthorityEvidence ev;
	ClusterTerminalAuthorityReason reason;
	bool terminal_seen = false;

	if (out_scn != NULL)
		*out_scn = InvalidScn;

	memset(&ev, 0, sizeof(ev));
	ev.enabled = cluster_cf_terminal_authority;
	ev.origin_node = origin_node;
	ev.xid = xid;
	ev.epoch_known = true;
	ev.observed_epoch = observed_epoch;
	ev.current_epoch = current_epoch;
	ev.owner_known = true;
	ev.owner_node = origin_node;
	ev.retention_required = retention_required;
	ev.retention_proven = retention_proven;
	ev.durable_commit_required = true;

	oc = cluster_remote_commit_outcome_ex(origin_node, xid, &outcome_scn, &outcome_wrap,
										  &outcome_wrap_valid);
	switch (oc) {
	case CLUSTER_REMOTE_XACT_COMMITTED:
		terminal_seen = true;
		ev.terminal_state = CLUSTER_TERMINAL_AUTH_COMMITTED;
		ev.terminal_scn = outcome_scn;

		/*
		 * Cross-check against the INDEPENDENT durable TT slot via an origin-
		 * qualified by-xid scan (NOT the offset path: the tuple's 8-slot heap ITL
		 * cache is reused, so its offset may point at a newer xact's durable
		 * slot).  The scan excludes any slot whose wrap differs from the outcome's
		 * (a same-xid wraparound), so exactly one resolved match whose commit_scn
		 * equals the outcome's is the authority; anything else is unprovable.
		 */
		if (outcome_wrap_valid
			&& cluster_tt_slot_durable_resolve_by_xid_origin(origin_node, xid, (uint32)outcome_wrap,
															 &durable_scn, NULL, NULL, NULL)
				   == CLUSTER_TT_DURABLE_RESOLVED_SCN) {
			ev.durable_commit_resolved = true;
			ev.durable_commit_scn = durable_scn;
		}
		break;

	case CLUSTER_REMOTE_XACT_ABORTED:
		terminal_seen = true;
		ev.terminal_state = CLUSTER_TERMINAL_AUTH_ABORTED;
		ev.terminal_scn = InvalidScn;
		ev.durable_commit_required = false;
		break;

	case CLUSTER_REMOTE_XACT_INDOUBT:
	default:
		ev.terminal_state = CLUSTER_TERMINAL_AUTH_UNKNOWN;
		ev.terminal_scn = InvalidScn;
		ev.durable_commit_required = false;
		break;
	}

	reason = cluster_terminal_authority_decide(&ev);
	cluster_terminal_authority_count_decision(reason);
	if (reason != CLUSTER_TERMINAL_AUTH_OK) {
		if (terminal_seen && RemoteXactShared != NULL)
			pg_atomic_fetch_add_u64(&RemoteXactShared->outcome_indoubt_count, 1);
		return CLUSTER_REMOTE_XACT_INDOUBT;
	}

	if (oc == CLUSTER_REMOTE_XACT_COMMITTED && out_scn != NULL)
		*out_scn = outcome_scn;
	return oc;
}

/*
 * cluster_remote_outcome_durable_checked -- compatibility wrapper for existing
 * Stage 4/5 callers.  With cluster.cf_terminal_authority=off (the spec-6.2
 * default), keep the legacy durable exact-match behavior.  Once the 6.2
 * terminal authority is explicitly enabled, route through the full evidence
 * decision using a stable current-epoch snapshot.
 */
ClusterRemoteXactOutcome
cluster_remote_outcome_durable_checked(int origin_node, TransactionId xid, SCN *out_scn)
{
	uint64 current_epoch = cluster_epoch_get_current();

	if (!cluster_cf_terminal_authority)
		return cluster_remote_outcome_durable_checked_legacy(origin_node, xid, out_scn);

	return cluster_remote_outcome_terminal_authorized(origin_node, xid, current_epoch,
													  current_epoch, false, true, out_scn);
}

/*
 * Extended authority read (spec-4.5a G6 P1 #2): additionally returns the
 * committing xact's TT wrap generation so a caller that holds an independent
 * durable TT slot can reject a same-xid wraparound overwrite.  out_wrap_valid
 * is false when the stored outcome carries no wrap (legacy / no tt_commit);
 * the caller MUST then fail closed for the exact-authority path (no bare
 * (origin,xid) trust -- 规则 8.A).
 */
ClusterRemoteXactOutcome
cluster_remote_commit_outcome_ex(int origin_node, TransactionId xid, SCN *commit_scn,
								 uint16 *out_wrap, bool *out_wrap_valid)
{
	int slotno;
	const ClusterRemoteXactEntry *entry;
	ClusterRemoteXactOutcome outcome;
	SCN scn;
	uint16 wrap;
	bool wrap_valid;

	if (commit_scn != NULL)
		*commit_scn = InvalidScn;
	if (out_wrap != NULL)
		*out_wrap = 0;
	if (out_wrap_valid != NULL)
		*out_wrap_valid = false;
	if (RemoteXactShared == NULL || origin_node < 0 || origin_node >= (1 << 7)
		|| !TransactionIdIsNormal(xid)) {
		if (RemoteXactShared != NULL)
			pg_atomic_fetch_add_u64(&RemoteXactShared->outcome_indoubt_count, 1);
		return CLUSTER_REMOTE_XACT_INDOUBT;
	}

	slotno = remote_xact_open_page(origin_node, xid, false);
	if (slotno < 0) {
		LWLockRelease(ClusterRemoteXactCtl->shared->ControlLock);
		pg_atomic_fetch_add_u64(&RemoteXactShared->outcome_indoubt_count, 1);
		return CLUSTER_REMOTE_XACT_INDOUBT;
	}
	entry = (const ClusterRemoteXactEntry *)(ClusterRemoteXactCtl->shared->page_buffer[slotno]
											 + (Size)cluster_remote_xact_entryno(xid)
												   * sizeof(ClusterRemoteXactEntry));
	outcome = (ClusterRemoteXactOutcome)entry->status;
	scn = entry->commit_scn;
	wrap = entry->wrap;
	wrap_valid = (entry->wrap_valid != 0);
	LWLockRelease(ClusterRemoteXactCtl->shared->ControlLock);

	if (outcome == CLUSTER_REMOTE_XACT_COMMITTED) {
		if (!SCN_VALID(scn)) {
			/* A committed entry without an SCN is corrupt -- fail closed. */
			pg_atomic_fetch_add_u64(&RemoteXactShared->outcome_indoubt_count, 1);
			return CLUSTER_REMOTE_XACT_INDOUBT;
		}
		if (commit_scn != NULL)
			*commit_scn = scn;
		if (out_wrap != NULL)
			*out_wrap = wrap;
		if (out_wrap_valid != NULL)
			*out_wrap_valid = wrap_valid;
		return CLUSTER_REMOTE_XACT_COMMITTED;
	}
	if (outcome == CLUSTER_REMOTE_XACT_ABORTED)
		return CLUSTER_REMOTE_XACT_ABORTED;

	pg_atomic_fetch_add_u64(&RemoteXactShared->outcome_indoubt_count, 1);
	return CLUSTER_REMOTE_XACT_INDOUBT;
}

void
cluster_remote_xact_flush(void)
{
	if (RemoteXactShared == NULL)
		return;
	SimpleLruWriteAll(ClusterRemoteXactCtl, true);
}

static bool
cluster_remote_xact_commit_wrap_proof(int origin_node, TransactionId xid,
									  const xl_xact_parsed_commit *parsed, int blocked_elevel,
									  bool require_wrap, uint16 *out_wrap)
{
	SCN durable_scn = InvalidScn;
	uint16 durable_wrap = 0;
	uint16 durable_seg = 0;
	uint16 durable_slot = 0;
	ClusterTTDurableResolve resolve;

	if (out_wrap != NULL)
		*out_wrap = 0;

	if (parsed->has_tt_commit) {
		cluster_tt_durable_redo_stamp_slot(parsed->tt_commit.instance, parsed->tt_commit.segment_id,
										   parsed->tt_commit.slot_offset, parsed->tt_commit.wrap,
										   parsed->tt_commit.xid, parsed->tt_commit.commit_scn);
		if (out_wrap != NULL)
			*out_wrap = parsed->tt_commit.wrap;
		return true;
	}

	resolve = cluster_tt_slot_durable_resolve_by_xid_origin(origin_node, xid, CLUSTER_TT_WRAP_ANY,
															&durable_scn, &durable_seg,
															&durable_slot, &durable_wrap);
	if (resolve == CLUSTER_TT_DURABLE_RESOLVED_SCN && durable_scn == parsed->scn) {
		if (out_wrap != NULL)
			*out_wrap = durable_wrap;
		return true;
	}

	if (require_wrap)
		ereport(blocked_elevel,
				(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
				 errmsg("merged recovery: foreign prepared commit lacks durable TT proof "
						"(origin node %d, xid %u)",
						origin_node, xid),
				 errdetail("durable_resolve=%d durable_scn=%llu commit_scn=%llu.", (int)resolve,
						   (unsigned long long)durable_scn, (unsigned long long)parsed->scn),
				 errhint("A foreign COMMIT PREPARED is mergeable only when the peer's "
						 "standalone durable-TT commit record was replayed before the "
						 "prepared-commit record.")));

	return false;
}

/*
 * cluster_remote_xact_apply -- D10b divert (P1-1 fail-closed parse).
 *
 *	online (spec-4.11 3b-2, R13): false = cold merged replay (startup) FATALs on
 *	an unmaterializable record; true = online thread recovery raises a CATCHABLE
 *	ERROR (cluster_remote_xact_blocked_elevel) so the R13 harness demotes it to
 *	BLOCKED and the survivor keeps running.  The materializing branches
 *	(cluster_remote_xact_set) run only under the orchestrator's online-writer
 *	scope (R14).
 */
void
cluster_remote_xact_apply(int origin_node, XLogReaderState *record, bool online)
{
	RmgrId rmid = XLogRecGetRmid(record);
	uint8 info = XLogRecGetInfo(record) & XLOG_XACT_OPMASK;
	TransactionId xid = XLogRecGetXid(record);
	int blocked_elevel = cluster_remote_xact_blocked_elevel(online);

	if (rmid == RM_MULTIXACT_ID || rmid == RM_COMMIT_TS_ID) {
		/*
		 * spec-4.5a G6 (F1 closure): foreign MULTIXACT/COMMIT_TS redo
		 * writes the LOCAL pg_multixact/pg_commit_ts by raw xid -- the
		 * same aliasing as pg_xact -- but this store has no per-origin
		 * representation for either.  Fail closed, never apply locally.
		 */
		ereport(
			blocked_elevel,
			(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
			 errmsg("merged recovery: foreign %s record cannot be materialized "
					"(origin node %d, xid %u)",
					rmid == RM_MULTIXACT_ID ? "multixact" : "commit-timestamp", origin_node, xid),
			 errhint("Cross-instance multixact / commit-timestamp state is not yet "
					 "mergeable; recover with cluster.merged_recovery=off.")));
	}

	if (rmid == RM_CLOG_ID) {
		/*
		 * CLOG records are page-extend/truncate housekeeping for B's OWN
		 * pg_xact.  This node's remote store derives outcomes from the
		 * XACT records themselves, so B's clog page maintenance carries
		 * no information here -- consume it without touching the LOCAL
		 * pg_xact (the whole point of the divert).
		 */
		return;
	}

	Assert(rmid == RM_XACT_ID);

	/*
	 * Only records keyed by a NORMAL xid materialize a per-origin outcome.
	 * COMMIT/ABORT PREPARED carry that xid in the parsed body, so they must
	 * bypass this raw-xl_xid guard and validate their parsed twophase_xid.
	 * Standalone invalidations/assignment sub-records carry nothing to key the
	 * store by -- consume them rather than open an SLRU page for xid 0.
	 */
	if (!TransactionIdIsNormal(xid) && info != XLOG_XACT_COMMIT_PREPARED
		&& info != XLOG_XACT_ABORT_PREPARED) {
		elog(DEBUG1,
			 "cluster_remote_xact_apply: consumed foreign XACT info 0x%02x with non-normal xid %u "
			 "(origin %d)",
			 (unsigned)info, xid, origin_node);
		return;
	}

	switch (info) {
	case XLOG_XACT_COMMIT: {
		xl_xact_commit *xlrec = (xl_xact_commit *)XLogRecGetData(record);
		xl_xact_parsed_commit parsed;
		uint16 commit_wrap = 0;
		bool commit_wrap_valid;

		ParseCommitRecord(XLogRecGetInfo(record), xlrec, &parsed);

		/*
		 * P1-1 hard rule: only a pure outcome may materialize.  Every
		 * cross-instance side effect xact_redo_commit would have driven
		 * (relfile drops, invalidations, stats drops, subxacts, 2PC,
		 * AE locks) is unsupported on a foreign stream -- fail closed,
		 * never silently dropped.
		 */
		if (cluster_remote_xact_commit_blocked(parsed.nrels, parsed.nmsgs, parsed.nstats,
											   parsed.nsubxacts, parsed.xinfo,
											   XACT_XINFO_HAS_TWOPHASE, XACT_XINFO_HAS_AE_LOCKS))
			ereport(
				blocked_elevel,
				(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
				 errmsg("merged recovery: foreign commit record carries an unsupported "
						"side effect (origin node %d, xid %u)",
						origin_node, xid),
				 errdetail("nrels=%d nmsgs=%d nstats=%d nsubxacts=%d xinfo=0x%x.", parsed.nrels,
						   parsed.nmsgs, parsed.nstats, parsed.nsubxacts, (unsigned)parsed.xinfo),
				 errhint("Cross-instance relfile drop / invalidation / stats / subxacts / "
						 "2PC are not yet mergeable (roadmap 4.6/4.7 + feature #11); "
						 "recover with cluster.merged_recovery=off.")));

		if (!SCN_VALID(parsed.scn))
			ereport(blocked_elevel,
					(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					 errmsg("merged recovery: foreign commit record carries no SCN "
							"(origin node %d, xid %u)",
							origin_node, xid),
					 errhint("spec-1.18 stamps every commit record; a missing SCN means "
							 "a pre-cluster WAL stream, which cannot be merged.")));

		/*
		 * Mirror the two cluster side effects xact_redo_commit would have
		 * driven for this record (the divert consumed it, so they must be
		 * replicated here -- everything ELSE in xact_redo_commit is what
		 * the divert exists to prevent):
		 *
		 *	1. Lamport observe (spec-1.18): A's SCN state must catch up to
		 *	   the peer's commit SCN, or post-recovery snapshots could read
		 *	   below a merged commit.
		 *	2. TT stamp fold (spec-3.18 D4.1): the commit record carries
		 *	   the durable TT slot delta; the redo stamp resolves its path
		 *	   from tt_commit.instance, so it lands in the MATERIALIZED
		 *	   pg_undo/instance_<origin> copy -- which is what the remote
		 *	   readers' commit_scn cross-check (G5) reads.
		 */
		cluster_scn_recovery_replay_observe(parsed.scn);
		commit_wrap_valid = cluster_remote_xact_commit_wrap_proof(
			origin_node, xid, &parsed, blocked_elevel, false, &commit_wrap);
		cluster_remote_xact_set(origin_node, xid, CLUSTER_REMOTE_XACT_COMMITTED, parsed.scn,
								commit_wrap_valid, commit_wrap);
		pg_atomic_fetch_add_u64(&RemoteXactShared->diverted_commit_count, 1);
		break;
	}
	case XLOG_XACT_COMMIT_PREPARED: {
		xl_xact_commit *xlrec = (xl_xact_commit *)XLogRecGetData(record);
		xl_xact_parsed_commit parsed;
		uint16 commit_wrap = 0;
		bool commit_wrap_valid;

		ParseCommitRecord(XLogRecGetInfo(record), xlrec, &parsed);
		xid = parsed.twophase_xid;
		if (!TransactionIdIsNormal(xid) || (parsed.xinfo & XACT_XINFO_HAS_TWOPHASE) == 0)
			ereport(blocked_elevel, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
									 errmsg("merged recovery: malformed foreign prepared commit "
											"(origin node %d, xid %u)",
											origin_node, xid),
									 errhint("The prepared transaction xid must be present in the "
											 "COMMIT PREPARED record body.")));

		if (cluster_remote_xact_commit_prepared_blocked(
				parsed.nrels, parsed.nmsgs, parsed.nstats, parsed.nsubxacts, parsed.xinfo,
				XACT_XINFO_HAS_TWOPHASE, XACT_XINFO_HAS_AE_LOCKS))
			ereport(
				blocked_elevel,
				(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
				 errmsg("merged recovery: foreign prepared commit carries an unsupported "
						"side effect (origin node %d, xid %u)",
						origin_node, xid),
				 errdetail("nrels=%d nmsgs=%d nstats=%d nsubxacts=%d xinfo=0x%x.", parsed.nrels,
						   parsed.nmsgs, parsed.nstats, parsed.nsubxacts, (unsigned)parsed.xinfo),
				 errhint("Cross-instance relfile drop / invalidation / stats / subxacts "
						 "are not yet mergeable; recover with "
						 "cluster.merged_recovery=off.")));

		if (!SCN_VALID(parsed.scn))
			ereport(blocked_elevel,
					(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					 errmsg("merged recovery: foreign prepared commit carries no SCN "
							"(origin node %d, xid %u)",
							origin_node, xid),
					 errhint("spec-1.18 stamps every prepared commit record; a missing SCN "
							 "means a pre-cluster WAL stream, which cannot be merged.")));

		cluster_scn_recovery_replay_observe(parsed.scn);
		commit_wrap_valid = cluster_remote_xact_commit_wrap_proof(
			origin_node, xid, &parsed, blocked_elevel, true, &commit_wrap);
		cluster_remote_xact_set(origin_node, xid, CLUSTER_REMOTE_XACT_COMMITTED, parsed.scn,
								commit_wrap_valid, commit_wrap);
		pg_atomic_fetch_add_u64(&RemoteXactShared->diverted_commit_count, 1);
		break;
	}
	case XLOG_XACT_ABORT: {
		xl_xact_abort *xlrec = (xl_xact_abort *)XLogRecGetData(record);
		xl_xact_parsed_abort parsed;

		ParseAbortRecord(XLogRecGetInfo(record), xlrec, &parsed);
		if (parsed.nrels > 0 || parsed.nstats > 0 || parsed.nsubxacts > 0
			|| (parsed.xinfo & XACT_XINFO_HAS_TWOPHASE) != 0)
			ereport(blocked_elevel,
					(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					 errmsg("merged recovery: foreign abort record carries an unsupported "
							"side effect (origin node %d, xid %u)",
							origin_node, xid),
					 errhint("Recover with cluster.merged_recovery=off.")));

		/* Mirror xact_redo_abort's Lamport observe (see the commit arm). */
		cluster_scn_recovery_replay_observe(parsed.scn);
		cluster_remote_xact_set(origin_node, xid, CLUSTER_REMOTE_XACT_ABORTED, InvalidScn, false,
								0);
		pg_atomic_fetch_add_u64(&RemoteXactShared->diverted_abort_count, 1);
		break;
	}
	case XLOG_XACT_ABORT_PREPARED: {
		xl_xact_abort *xlrec = (xl_xact_abort *)XLogRecGetData(record);
		xl_xact_parsed_abort parsed;

		ParseAbortRecord(XLogRecGetInfo(record), xlrec, &parsed);
		xid = parsed.twophase_xid;
		if (!TransactionIdIsNormal(xid) || (parsed.xinfo & XACT_XINFO_HAS_TWOPHASE) == 0)
			ereport(blocked_elevel, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
									 errmsg("merged recovery: malformed foreign prepared abort "
											"(origin node %d, xid %u)",
											origin_node, xid),
									 errhint("The prepared transaction xid must be present in the "
											 "ABORT PREPARED record body.")));
		if (parsed.nrels > 0 || parsed.nstats > 0 || parsed.nsubxacts > 0)
			ereport(blocked_elevel,
					(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					 errmsg("merged recovery: foreign prepared abort carries an unsupported "
							"side effect (origin node %d, xid %u)",
							origin_node, xid),
					 errhint("Recover with cluster.merged_recovery=off.")));

		cluster_scn_recovery_replay_observe(parsed.scn);
		cluster_remote_xact_set(origin_node, xid, CLUSTER_REMOTE_XACT_ABORTED, InvalidScn, false,
								0);
		pg_atomic_fetch_add_u64(&RemoteXactShared->diverted_abort_count, 1);
		break;
	}
	case XLOG_XACT_PREPARE: {
		xl_xact_prepare *xlrec = (xl_xact_prepare *)XLogRecGetData(record);
		xl_xact_parsed_prepare parsed;

		ParsePrepareRecord(XLogRecGetInfo(record), xlrec, &parsed);
		xid = parsed.twophase_xid;
		if (!TransactionIdIsNormal(xid))
			ereport(blocked_elevel, (errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
									 errmsg("merged recovery: malformed foreign prepare "
											"(origin node %d, xid %u)",
											origin_node, xid)));
		if (parsed.nsubxacts > 0 || parsed.nrels > 0 || parsed.nabortrels > 0 || parsed.nstats > 0
			|| parsed.nabortstats > 0 || parsed.nmsgs > 0 || xlrec->initfileinval)
			ereport(blocked_elevel,
					(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
					 errmsg("merged recovery: foreign prepare carries an unsupported "
							"side effect (origin node %d, xid %u)",
							origin_node, xid),
					 errdetail("nsubxacts=%d ncommitrels=%d nabortrels=%d ncommitstats=%d "
							   "nabortstats=%d ninvalmsgs=%d initfileinval=%s.",
							   parsed.nsubxacts, parsed.nrels, parsed.nabortrels, parsed.nstats,
							   parsed.nabortstats, parsed.nmsgs,
							   xlrec->initfileinval ? "true" : "false"),
					 errhint("Foreign prepared transactions are not exposed as local prepared "
							 "transactions; unresolved prepares remain in-doubt until their "
							 "COMMIT PREPARED or ABORT PREPARED record is replayed.")));

		/*
		 * Never call PrepareRedoAdd for a foreign stream: that would expose a
		 * peer's prepared xact in this node's local pg_prepared_xacts namespace
		 * and alias raw xids.  PREPARE is not a durable outcome, either: until a
		 * matching COMMIT/ABORT PREPARED record is replayed, the remote xid must
		 * remain INDOUBT so tuple visibility fails closed instead of inventing an
		 * ABORTED verdict for an in-doubt prepared transaction.
		 */
		break;
	}
	default:
		/*
		 * ASSIGNMENT / INVALIDATIONS with normal xl_xid: all carry cross-instance
		 * machinery this store cannot honestly absorb yet.
		 */
		ereport(blocked_elevel,
				(errcode(ERRCODE_CLUSTER_MERGED_RECOVERY_BLOCKED),
				 errmsg("merged recovery: unsupported foreign xact record (info 0x%02x, "
						"origin node %d, xid %u)",
						(unsigned)info, origin_node, xid),
				 errhint("Assignment / standalone-invalidation records are not yet mergeable; "
						 "recover with cluster.merged_recovery=off.")));
	}
}

uint64
cluster_remote_xact_diverted_commit_count(void)
{
	return RemoteXactShared ? pg_atomic_read_u64(&RemoteXactShared->diverted_commit_count) : 0;
}
uint64
cluster_remote_xact_diverted_abort_count(void)
{
	return RemoteXactShared ? pg_atomic_read_u64(&RemoteXactShared->diverted_abort_count) : 0;
}
uint64
cluster_remote_xact_outcome_indoubt_count(void)
{
	return RemoteXactShared ? pg_atomic_read_u64(&RemoteXactShared->outcome_indoubt_count) : 0;
}

#endif /* USE_PGRAC_CLUSTER */
