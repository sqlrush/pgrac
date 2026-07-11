/*-------------------------------------------------------------------------
 *
 * cluster_undo_authority_snapshot.c
 *	  Live membership-snapshot wrapper for dead/absent-owner undo
 *	  serve-authority election (spec-5.22d D4-1, wrapper half).
 *
 *	  Reads the real membership-state SSOT (cluster_membership_get_state),
 *	  heartbeat freshness (cluster_reconfig_get_observed_fresh_alive, the
 *	  L420 freshness gate -- NOT record existence), and the current accepted
 *	  reconfig epoch (cluster_epoch_get_current), projects them into a
 *	  ClusterUndoAuthorityInput, and defers the actual election to the pure
 *	  cluster_undo_authority_decide().  Kept out of cluster_undo_authority.c
 *	  so the pure decision object links standalone in the D4-1 unit test;
 *	  this wrapper is covered by the D4-8 TAP.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_undo_authority_snapshot.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-5.22d-undo-dead-owner-verdict-serve.md (D4-1, §2.1)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_conf.h"				/* CLUSTER_MAX_NODES */
#include "cluster/cluster_cr.h"					/* undo_authority_* counters (D4-5) */
#include "cluster/cluster_epoch.h"				/* cluster_epoch_get_current */
#include "cluster/cluster_inject.h"				/* D4-8 L3 prove-refusal injection */
#include "cluster/cluster_membership.h"			/* cluster_membership_get_state */
#include "cluster/cluster_reconfig.h"			/* cluster_reconfig_get_observed_fresh_alive */
#include "cluster/cluster_runtime_visibility.h" /* cluster_vis_tt_block_positive_proof */
#include "cluster/cluster_scn.h"				/* SCN_MAX_VALID_NODE_ID */
#include "cluster/cluster_tt_slot.h"			/* TT_WRAP_INVALID */
#include "cluster/cluster_undo_authority.h"
#include "cluster/cluster_undo_resid.h"		   /* cluster_undo_resid_master (D1) */
#include "cluster/cluster_undo_smgr.h"		   /* cluster_undo_smgr_read_block (D4-5) */
#include "cluster/storage/cluster_shared_fs.h" /* undo_instance_dir_resolve (A1/D4-8) */
#include "storage/fd.h"						   /* AllocateDir/ReadDirExtended (A1/D4-8) */

#ifdef USE_PGRAC_CLUSTER

static inline void
bm_set(uint8 *bm, int32 node)
{
	bm[node >> 3] |= (uint8)(1u << (node & 7));
}

/*
 * cluster_undo_serve_authority_lookup -- live-snapshot wrapper.
 *
 *	Projects the membership SSOT into the pure decide()'s bitmaps:
 *	  alive_fresh  <- state==MEMBER AND observed_fresh_alive (L420)
 *	  dead_decided <- state==DEAD or state==REMOVED (durable fail-stop /
 *	                  decommission decision; NOT a transient live read, L419)
 *	  declared     <- every non-absent, non-rejected known node
 *	ABSENT / JOINING / REJECTED owners are neither fresh nor dead-decided,
 *	so decide() fails closed (UNKNOWN) -- we never guess a node dead.
 */
ClusterUndoAuthorityStatus
cluster_undo_serve_authority_lookup(int32 owner_node, uint64 reconfig_epoch, int32 *out_authority)
{
	ClusterUndoAuthorityInput in;
	ClusterUndoAuthorityDecision out;
	int32 node;

	*out_authority = -1;

	memset(&in, 0, sizeof(in));
	in.owner_node = owner_node;
	in.request_epoch = reconfig_epoch;
	in.snapshot_epoch = cluster_epoch_get_current();

	for (node = 0; node <= SCN_MAX_VALID_NODE_ID && node < CLUSTER_MAX_NODES; node++) {
		ClusterMembershipState st = cluster_membership_get_state(node);

		switch (st) {
		case CLUSTER_MEMBER_MEMBER:
			bm_set(in.declared, node);
			if (cluster_reconfig_get_observed_fresh_alive(node))
				bm_set(in.alive_fresh, node);
			break;
		case CLUSTER_MEMBER_DEAD:
		case CLUSTER_MEMBER_REMOVED:
			bm_set(in.declared, node);
			bm_set(in.dead_decided, node);
			break;
		case CLUSTER_MEMBER_JOINING:
			/* known/declared but not eligible either way (may become live) */
			bm_set(in.declared, node);
			break;
		case CLUSTER_MEMBER_ABSENT:
		case CLUSTER_MEMBER_REJECTED:
		default:
			/* not a usable authority; owner here => decide() UNKNOWN */
			break;
		}
	}

	(void)cluster_undo_authority_decide(&in, &out);
	if (out.status == CLUSTER_UNDO_AUTHORITY_OK)
		*out_authority = out.authority_node;
	return out.status;
}

/*
 * cluster_undo_serve_authority -- resolve the serve route for one undo
 * resource (D4-2 heavy glue).
 *
 *	Canonical owner comes from the D1 pure identity (cluster_undo_resid_
 *	master, never a hash route); the live serve authority comes from the
 *	D4-1 wrapper; the pure D4-2 mapper turns the two into a route.  A lookup
 *	miss yields a fail-closed route (destination -1), NEVER the GRD hash
 *	master (spec-5.22d 约束 #2 / §2.2).
 */
ClusterUndoServeRoute
cluster_undo_serve_authority(const ClusterResId *undo_resid, uint64 reconfig_epoch)
{
	int32 owner_node = cluster_undo_resid_master(undo_resid);
	int32 authority_node = -1;
	ClusterUndoAuthorityStatus st;

	st = cluster_undo_serve_authority_lookup(owner_node, reconfig_epoch, &authority_node);
	return cluster_undo_route_decide(owner_node, reconfig_epoch, st, authority_node);
}

/*
 * authority_scan_owner_segments -- A1 (D4-8) durable segment-set scan.
 *
 *	Enumerate the dead owner's SHARED undo directory (the one namespace
 *	cluster_shared_fs_undo_path_resolve ever writes into — A1.1) and fold
 *	every segment's block0 xid scan into *agg.  完备-或-fail-closed: any
 *	enumeration break, non-canonical entry name, unreadable block0 or
 *	unparseable block poisons the whole set — the caller must refuse, never
 *	skip-and-continue (a uniqueness claim needs the full set visible).
 *
 *	Writer exclusion (A1.1-bis #1) makes the enumerated set stable: the dead
 *	owner is dead-decided + write-fenced (its revival forces a reconfig ⇒
 *	epoch bump ⇒ the caller's post-scan re-check refuses); survivors' only
 *	foreign-owner intent is AUTHORITY_BLOCK0, whose single I/O consumer is
 *	the read below (no writer, no unlink/rename path exists).  spec-5.22e
 *	(D5) RULED the reclaimer question: retention stays own-instance only
 *	(dead-owner segments are frozen; the path_resolve ownership guard is
 *	now enforced in release builds too), so this invariant holds unchanged.
 *	A future cross-node reclaimer (D7+) must revise the A1.1-bis argument
 *	(serialize against authority serve) BEFORE deleting anything here.
 *
 *	seg_0 is NOT skipped (A1.1-bis #3): "segment 0 never carries a xact TT
 *	slot" is not a verified structural invariant (the uba_encode Assert only
 *	fences record UBAs, and the slot cursor rolls independently), so if a
 *	shared seg_0 exists it is verified and counted like any other.
 *
 *	true = the whole set was enumerated + parsed (agg holds the aggregate);
 *	false = the set is not provably complete (caller refuses; the caller
 *	owns the scan_incomplete counter so refusal attribution has one home).
 */
static bool
authority_scan_owner_segments(int32 owner_node, TransactionId raw_xid,
							  ClusterUndoAuthorityScanAgg *agg)
{
	char dirpath[MAXPGPATH];
	DIR *dir;
	struct dirent *de;
	uint8 owner_instance = (uint8)(owner_node + 1);
	volatile bool complete = true;
	MemoryContext scan_ctx = CurrentMemoryContext;

	if (cluster_shared_fs_undo_instance_dir_resolve(owner_instance, dirpath, sizeof(dirpath)) != 0)
		return false;

	dir = AllocateDir(dirpath);
	if (dir == NULL)
		return false;

	/*
	 * 完备-或-fail-closed enumeration (spec-5.22d Hardening, errno-fragility):
	 * ReadDir uses the ERROR elevel, so a directory read that breaks
	 * mid-enumeration THROWS rather than returning NULL indistinguishably from
	 * end-of-directory.  The pre-fix errno-after-loop probe was unreliable --
	 * ereport can clobber errno on the LOG path, so an EIO-truncated scan could
	 * be judged "complete" and serve a false-unique verdict.  The PG_TRY folds
	 * any throw into the same coverage failure the parse/read paths below
	 * already signal, so BOTH the SELF reader leg and the LMS wire leg fail
	 * closed identically (Rule 8.A); FreeDir runs on every path (no AllocateDir
	 * leak on the wire leg's error-swallowing drain envelope).
	 */
	PG_TRY();
	{
		while ((de = ReadDir(dir, dirpath)) != NULL) {
			char canon[64];
			unsigned long id;
			char *end;
			PGAlignedBlock page;
			int nmatch = 0;
			ClusterVisTtProof proof = CLUSTER_VIS_TT_PROOF_NONE;
			SCN scn = InvalidScn;
			uint16 wrap = TT_WRAP_INVALID;

			/*
			 * spec-5.22d Hardening (errno-fragility): model a mid-enumeration
			 * directory read failure.  Armed ERROR throws here exactly as a real
			 * ReadDir I/O error would; the enclosing PG_TRY folds it into a
			 * coverage failure, never a truncated "complete" scan.
			 */
			CLUSTER_INJECTION_POINT("cluster-undo-authority-scan");

			if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
				continue;

			/*
			 * Canonical name gate (A1.1-bis #2): every other entry must be
			 * EXACTLY seg_<id>.dat as the writer renders it — parse, then
			 * re-render and compare, so aliases (seg_007.dat), trailing junk
			 * and out-of-range ids all poison the set instead of hiding
			 * evidence under a name the scan would skip.
			 */
			if (strncmp(de->d_name, "seg_", 4) != 0) {
				complete = false;
				break;
			}
			errno = 0;
			id = strtoul(de->d_name + 4, &end, 10);
			if (errno != 0 || end == de->d_name + 4 || strcmp(end, ".dat") != 0
				|| id > UINT16_MAX) {
				complete = false;
				break;
			}
			snprintf(canon, sizeof(canon), "seg_%lu.dat", id);
			if (strcmp(canon, de->d_name) != 0) {
				complete = false;
				break;
			}

			/* Read + parse this segment's block0; any failure poisons the set. */
			if (!cluster_undo_smgr_read_block(CLUSTER_UNDO_PATH_RUNTIME_SHARED_AUTHORITY_BLOCK0,
											  (uint32)id, owner_instance, 0 /* block0 */,
											  page.data)) {
				complete = false;
				break;
			}
			if (cluster_vis_tt_block_xid_scan(page.data, (uint32)id, owner_instance, raw_xid,
											  &nmatch, &proof, &scn, &wrap)
				!= CLUSTER_VIS_TT_BLOCK_SCAN_OK) {
				complete = false;
				break;
			}
			cluster_undo_authority_scan_fold(agg, true, nmatch, proof, scn, wrap);
		}
	}
	PG_CATCH();
	{
		/*
		 * A directory read fault (or any throw) during enumeration means the
		 * set is not provably complete: fold it to a coverage failure and keep
		 * the reader backend / LMS worker alive (the caller bumps
		 * scan_incomplete_reject + fail_closed).  cluster_undo_smgr_read_block
		 * never throws -- it returns false, handled above -- so in practice
		 * this catches the ReadDir enumeration fault; either way an
		 * un-completable scan can never masquerade as a complete one.
		 */
		MemoryContextSwitchTo(scan_ctx);
		FlushErrorState();
		complete = false;
	}
	PG_END_TRY();

	FreeDir(dir);
	return complete;
}

/*
 * cluster_undo_authority_block0_prove -- shared authority prove core (D4-5;
 * A1/D4-8 widened from single-segment to the owner's COMPLETE durable
 * segment set).  See cluster_undo_authority.h for the contract.
 *
 *	The live CP5 verdict is a complete scan over the origin's LIVE TT
 *	because a single block0's 0-match proves nothing — the xid's slot may
 *	live in another segment (the slot cursor rolls independently of the
 *	record cursor the ref's UBA points at; D4-8 e2e evidence).  For a DEAD
 *	owner the durable shared segment set is the same authority translated
 *	to durable state, so THIS is the complete scan: enumerate every shared
 *	segment (A1.1), demand the whole set parseable, and serve only a
 *	set-wide UNIQUE terminal match (truth table A1.2).
 *
 *	Coverage discipline, counters owned HERE (self + wire legs can never
 *	drift):
 *	  (i)   claimed_at_epoch: claim_epoch is the current accepted epoch at
 *	        entry AND STILL at scan end (A1 约束 3 — a reconfig mid-scan
 *	        could otherwise stitch a cross-epoch verdict) — else
 *	        epoch_stale_reject + fail_closed.
 *	  (ii)  set complete: enumeration + every block0 read + parse succeeded
 *	        — else scan_incomplete_reject + fail_closed (A1 约束 1).
 *	  (iii) unique terminal match: exactly one xid+wrap match across the
 *	        whole set, terminal (COMMITTED-with-scn / ABORTED) — 0-match and
 *	        non-terminal fail_closed; >=2 matches multi_match_reject +
 *	        fail_closed (A1 约束 2).
 *	block0 bytes are synchronously durable (D2 Q3; the commit/abort stamps
 *	are targeted pre-commit pwrites), so full reads are crash-consistent —
 *	no LSN-cover window applies.  A regular (non-2PC) abort leaves its slot
 *	durably ACTIVE (no durable ABORTED stamp exists outside 2PC and the
 *	owner's own recovery, A1.5): such xids stay in-doubt here by design.
 */
ClusterUndoVerdictResult
cluster_undo_authority_block0_prove(int32 owner_node, uint32 segment_id, TransactionId raw_xid,
									uint64 claim_epoch)
{
	ClusterUndoVerdictResult unknown
		= { .kind = CLUSTER_UNDO_VERDICT_UNKNOWN_FAIL_CLOSED, .commit_scn = InvalidScn, .wrap = 0 };
	ClusterUndoAuthorityScanAgg agg;

	/* malformed owner: never provable (defense in depth; the route layers
	 * upstream already refuse it) */
	if (owner_node < 0 || owner_node > SCN_MAX_VALID_NODE_ID || !TransactionIdIsNormal(raw_xid)) {
		cluster_undo_authority_note_failclosed();
		return unknown;
	}

	/* Malformed-ref precondition ONLY (A1.2): the ref's segment_id no longer
	 * scopes the scan (the slot may live in any segment), but a ref carrying
	 * the bootstrap segment or an over-range id is not resolvable evidence. */
	if (segment_id == 0 || segment_id > UINT16_MAX) {
		cluster_undo_authority_note_failclosed();
		return unknown;
	}

	/* spec-5.22d D4-8 L3 (L408): SKIP forces the coverage-fail refusal under
	 * a topology that would otherwise serve — proves the fail-closed arm and
	 * its counter really fire (53R97, never a native answer).  Inert unless
	 * armed; arm state is process-local, which reaches the SELF leg (this
	 * runs in the reader backend); the wire leg runs in LMS, where arming is
	 * the conf-time chaos-harness face (t/015 note). */
	CLUSTER_INJECTION_POINT("cluster-undo-authority-block0-prove");
	if (cluster_injection_should_skip("cluster-undo-authority-block0-prove")) {
		cluster_undo_authority_note_failclosed();
		return unknown;
	}

	/* (i) the claim must be scoped to the CURRENT epoch at entry */
	if (cluster_epoch_get_current() != claim_epoch) {
		cluster_undo_authority_note_epoch_stale_reject();
		cluster_undo_authority_note_failclosed();
		return unknown;
	}

	/* (ii) enumerate + parse the owner's COMPLETE durable segment set */
	cluster_undo_authority_scan_agg_init(&agg);
	if (!authority_scan_owner_segments(owner_node, raw_xid, &agg) || agg.poisoned) {
		cluster_undo_authority_note_scan_incomplete_reject();
		cluster_undo_authority_note_failclosed();
		return unknown;
	}

	/* (i-bis) the epoch must not have moved ACROSS the scan window (A1 约束
	 * 3): the writer-exclusion argument for the enumerated set is scoped to
	 * one epoch, so a mid-scan reconfig voids the set's stability. */
	if (cluster_epoch_get_current() != claim_epoch) {
		cluster_undo_authority_note_epoch_stale_reject();
		cluster_undo_authority_note_failclosed();
		return unknown;
	}

	/* (iii) set-wide unique terminal match, or refuse */
	if (agg.total_match >= 2) {
		cluster_undo_authority_note_multi_match_reject();
		cluster_undo_authority_note_failclosed();
		return unknown;
	}
	if (!cluster_undo_authority_scan_admissible(&agg)) {
		cluster_undo_authority_note_failclosed();
		return unknown;
	}

	cluster_undo_authority_note_serve_hit();
	return cluster_undo_verdict_from_block_proof(agg.proof, agg.commit_scn, agg.wrap);
}

#endif /* USE_PGRAC_CLUSTER */
