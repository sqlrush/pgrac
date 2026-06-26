/*-------------------------------------------------------------------------
 *
 * cluster_tt_2pc.c
 *	  pgrac cluster TT state in two-phase commit (spec-3.15).
 *
 *	  Layering (L212): the serialize/parse pair below is pure (no
 *	  allocation, no ereport) and cluster_unit-enumerable; the
 *	  AtPrepare/PostPrepare shells and the twophase callbacks stay thin
 *	  and never hand-copy the record layout.
 *
 *	  Step 2 scope: record serialize/parse + AtPrepare/PostPrepare
 *	  shells.  The rmgr callbacks (recover/postcommit/postabort) land at
 *	  step 3; the prefinish resolve lands at steps 6-7.  Until step 4
 *	  wires the shells into xact.c, nothing here is reachable (the
 *	  spec-3.5/3.7 PREPARE guards stay in place -- C-P1).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_tt_2pc.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-3.15-2pc-prepared-visibility.md (FROZEN v0.2).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "access/twophase.h"
#include "access/twophase_rmgr.h"

#include "cluster/cluster_guc.h"
#include "cluster/cluster_itl_touch.h" /* PostPrepare touch-list drop (V-2) */
#include "cluster/cluster_subtrans.h"  /* sub-link export/reset (D7) */
#include "cluster/cluster_tt_2pc.h"
#include "cluster/cluster_tt_local.h"		 /* binding export/reset */
#include "cluster/cluster_tt_slot.h"		 /* protected-slot map (D6/V-4) */
#include "cluster/cluster_tt_status.h"		 /* SUBCOMMITTED overlay rebuild */
#include "cluster/cluster_undo_record_api.h" /* xact_reset (PostPrepare) */
#include "cluster/cluster_tt_durable.h"		 /* durable 0x30 commit (prefinish) */
#include "cluster/cluster_tt_status_hint.h"	 /* cross-node hint emit (prefinish) */


/*
 * AtPrepare_ClusterTT -- spec-3.15 D1/D3 (PREPARE serialize-only shell).
 *
 *	PG two-phase contract: AtPrepare routines may error out but MUST NOT
 *	mutate backend-local state (an EndPrepare failure aborts the xact,
 *	and the abort path still needs the bindings to release shmem slots).
 *	State transfer happens in PostPrepare_ClusterTT after EndPrepare
 *	succeeded.
 *
 *	Read-only / zero-cluster-state PREPARE registers nothing (Q5).
 */
void
AtPrepare_ClusterTT(void)
{
	ClusterTT2PCBinding bindings[CLUSTER_TT_2PC_MAX_BINDINGS + 1];
	/* PGRAC: zero-init (only heads[0..nbindings-1] is filled+read; the
	 * defensive {0} silences a cppcheck uninitvar surfaced by spec-5.14's
	 * new cluster_touched_peers.c in the same scan dir). */
	UBA heads[CLUSTER_TT_2PC_MAX_BINDINGS + 1] = { 0 };
	ClusterTT2PCSubLink sublinks[CLUSTER_TT_2PC_MAX_SUBLINKS + 1];
	uint16 nbindings;
	uint32 nsublinks;
	uint32 len;
	char *buf;
	uint16 i;

	if (!cluster_enabled || cluster_node_id < 0)
		return;

	nbindings = cluster_tt_local_export_bindings(bindings, CLUSTER_TT_2PC_MAX_BINDINGS + 1);
	nsublinks = cluster_subtrans_export_links(sublinks, CLUSTER_TT_2PC_MAX_SUBLINKS + 1);

	if (nbindings == 0 && nsublinks == 0)
		return; /* Q5: nothing to carry */

	/*
	 * spec-4.8 D7-A: capture each binding's latest undo-chain head into the v2
	 * heads[] section.  The head is backend-local (cluster_undo_record.c) and is
	 * reset at xact end, so it must be read here at PREPARE while still live;
	 * the durable TT slot header does not persist it (deferred).  A binding with
	 * no undo this xact gets InvalidUba (-> D7 physical rollback no-op for it).
	 */
	for (i = 0; i < nbindings && i <= CLUSTER_TT_2PC_MAX_BINDINGS; i++)
		heads[i] = cluster_undo_local_head_get((uint16)bindings[i].undo_segment_id,
											   bindings[i].slot_offset);

	/*
	 * §1.4-4 capacity: export functions return count+1 saturated when the
	 * backend-local state exceeds the cap, which serialize() rejects --
	 * surface it as a clean PREPARE failure, not silent truncation.
	 */
	len = cluster_tt_2pc_record_size(CLUSTER_TT_2PC_VERSION,
									 Min(nbindings, CLUSTER_TT_2PC_MAX_BINDINGS),
									 Min(nsublinks, CLUSTER_TT_2PC_MAX_SUBLINKS));
	buf = palloc(len);
	if (cluster_tt_2pc_serialize(bindings, heads, nbindings, sublinks, nsublinks, buf, len) == 0)
		ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						errmsg("cannot PREPARE a transaction with more than %d cluster TT bindings "
							   "or %d subtransaction links",
							   CLUSTER_TT_2PC_MAX_BINDINGS, CLUSTER_TT_2PC_MAX_SUBLINKS),
						errhint("Split the transaction, or resolve it without two-phase commit.")));

	RegisterTwoPhaseRecord(TWOPHASE_RM_CLUSTER_TT_ID, 0, buf, len);
	cluster_vis_bump_twopc_prepare_records();
	pfree(buf);
}


/*
 * PostPrepare_ClusterTT -- ownership transfer after EndPrepare succeeded.
 *
 *	The 2PC record is now the single authority (C-P4): drop the backend-
 *	local TT bindings, the SUBCOMMITTED link list, and the ITL touch
 *	list (V-2: droppable -- overlay/durable TT are authoritative and the
 *	3.4c lazy cleanout re-stamps page ITLs on later reads).  Nothing
 *	here touches shmem or durable state.
 */
void
PostPrepare_ClusterTT(void)
{
	if (!cluster_enabled || cluster_node_id < 0)
		return;

	cluster_tt_local_reset_binding();
	cluster_subtrans_reset_local_links();
	cluster_itl_touch_reset_at_end_xact();
	cluster_undo_record_xact_reset(); /* touched-undo list + D16 flag */
	cluster_vis_bump_twopc_postprepare_transfers();
}


/*
 * parse_or_corrupt -- shared fail-closed record validation for the
 * twophase callbacks (C-P7 family: a damaged 2PC record must stop the
 * resolve loudly, mirroring PG's treatment of a damaged state file).
 */
static void
parse_or_corrupt(TransactionId xid, const void *recdata, uint32 len, ClusterTT2PCParsed *out)
{
	if (!cluster_tt_2pc_parse_record(recdata, len, out))
		ereport(ERROR, (errcode(ERRCODE_DATA_CORRUPTED),
						errmsg("corrupt cluster TT 2PC record for prepared transaction %u (len %u)",
							   xid, len),
						errhint("The two-phase state file payload failed magic/version/length/CRC "
								"validation.")));
}


/*
 * cluster_tt_twophase_recover -- spec-3.15 D6 (crash-restart re-pin).
 *
 *	Runs from RecoverPreparedTransactions before user transactions are
 *	accepted.  Re-pins every binding into the protected-slot map so the
 *	allocator cannot hand a prepared xact's slot to a new transaction
 *	(V-4), and rebuilds the SUBCOMMITTED overlay links (the shmem
 *	overlay did not survive the crash).  Idempotent (C-P2): protect()
 *	short-circuits on an identical tuple and the overlay install is
 *	keyed.
 */
void
cluster_tt_twophase_recover(TransactionId xid, uint16 info, void *recdata, uint32 len)
{
	ClusterTT2PCParsed p;
	uint16 i;
	uint32 j;

	(void)info;
	parse_or_corrupt(xid, recdata, len, &p);
	cluster_vis_bump_twopc_recover_rebinds();

	for (i = 0; i < p.nbindings; i++) {
		const ClusterTT2PCBinding *b = &p.bindings[i];

		if (!cluster_tt_slot_protect(b->undo_segment_id, b->slot_offset, b->wrap, b->xid))
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("cannot re-pin prepared transaction %u TT slot (segment %u, slot %u)",
							b->xid, b->undo_segment_id, b->slot_offset),
					 errhint("Protected-slot map is full or the slot is pinned by a conflicting "
							 "owner; resolve other prepared transactions first.")));
	}

	for (j = 0; j < p.nsublinks; j++) {
		const ClusterTT2PCSubLink *l = &p.sublinks[j];

		if (!cluster_tt_status_install_subcommitted(&l->child_key, &l->parent_key))
			ereport(ERROR, (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
							errmsg("cannot rebuild SUBCOMMITTED overlay for prepared "
								   "transaction %u (subxid %u)",
								   xid, l->child_key.local_xid),
							errhint("TT status overlay is full; increase its capacity or "
									"resolve prepared transactions.")));
	}
}


/*
 * postcommit / postabort -- non-fallible cleanup only (C-P7).
 *
 *	The durable resolve already happened in the prefinish hook (steps
 *	6-7) BEFORE the prepared commit/abort WAL.  Here we only drop the
 *	protected-slot pins for this xid; idempotent and safe on absence
 *	(unprotect of an unknown xid is a no-op).
 */
void
cluster_tt_twophase_postcommit(TransactionId xid, uint16 info, void *recdata, uint32 len)
{
	(void)info;
	(void)recdata;
	(void)len;
	(void)cluster_tt_slot_unprotect_xid(xid);
}

void
cluster_tt_twophase_postabort(TransactionId xid, uint16 info, void *recdata, uint32 len)
{
	(void)info;
	(void)recdata;
	(void)len;
	(void)cluster_tt_slot_unprotect_xid(xid);
}


/*
 * cluster_tt_twophase_standby_recover -- spec-3.16 D1.
 *
 *	Hot-standby received this prepared xact via the WAL stream.  Rebuild
 *	its TT status overlay so a standby reader resolves the prepared
 *	tuples as IN_PROGRESS instead of overlay-miss 53R97.  Standby is
 *	read-only and allocates no slots, so -- unlike the crash-restart
 *	recover callback -- there is NO protected-slot map step (V-4 / C-R3).
 *
 *	Failure semantics (spec-3.16 hardening v0.2):
 *	  - corrupt 2PC payload (magic/version/length/CRC) is corrupted WAL/
 *	    state: fail-loud (DATA_CORRUPTED ERROR), same as the crash-restart
 *	    recover callback -- the standby must NOT silently swallow it;
 *	  - overlay capacity / shmem unavailable: WARNING + count, and the
 *	    affected prepared reads 53R97 fail-closed on the standby.
 *
 *	V-3 note: the overlay is NOT updated when the standby later replays
 *	COMMIT PREPARED (0x30 redo touches only the durable header) -- full
 *	standby reader visibility is forward-linked to #95.
 */
void
cluster_tt_twophase_standby_recover(TransactionId xid, uint16 info, void *recdata, uint32 len)
{
	ClusterTT2PCParsed p;
	uint16 i;
	uint32 j;

	(void)info;

	if (!cluster_enabled || cluster_node_id < 0)
		return;

	/* corrupt payload -> fail-loud (same as crash-restart recover). */
	parse_or_corrupt(xid, recdata, len, &p);

	for (i = 0; i < p.nbindings; i++) {
		const ClusterTT2PCBinding *b = &p.bindings[i];
		ClusterTTStatusKey key;

		memset(&key, 0, sizeof(key));
		key.origin_node_id = (uint16)cluster_node_id;
		key.undo_segment_id = (uint16)b->undo_segment_id;
		key.tt_slot_id = cluster_tt_slot_offset_to_id(b->slot_offset);
		key.cluster_epoch = b->cluster_epoch;
		key.local_xid = b->xid;

		if (!cluster_tt_status_install_local(&key, CLUSTER_TT_STATUS_IN_PROGRESS, InvalidScn)) {
			/* capacity / shmem unavailable: degrade, do NOT PANIC the
			 * standby; affected reads fail-closed 53R97 + we count it. */
			cluster_vis_bump_recovery_overlay_rebuild_count();
			ereport(WARNING,
					(errmsg("cluster standby: TT overlay full rebuilding prepared transaction "
							"%u (xid %u); affected reads will fail-closed",
							xid, b->xid)));
			return;
		}
	}

	for (j = 0; j < p.nsublinks; j++) {
		const ClusterTT2PCSubLink *l = &p.sublinks[j];

		if (!cluster_tt_status_install_subcommitted(&l->child_key, &l->parent_key)) {
			/* capacity / shmem unavailable: degrade, do NOT PANIC the
			 * standby; affected reads fail-closed 53R97 + we count it. */
			cluster_vis_bump_recovery_overlay_rebuild_count();
			ereport(WARNING,
					(errmsg("cluster standby: SUBCOMMITTED overlay full rebuilding prepared "
							"transaction %u (subxid %u); affected reads will fail-closed",
							xid, l->child_key.local_xid)));
			return;
		}
	}

	cluster_vis_bump_recovery_2pc_standby_rebuilds();
}


/*
 * cluster_tt_twophase_prefinish -- spec-3.15 D5 (C-P6 resolve-before-WAL).
 *
 *	Runs inside FinishPreparedTransaction AFTER final_scn is produced
 *	and BEFORE RecordTransactionCommitPrepared/AbortPrepared.  The
 *	backend-local bindings are long gone (PostPrepare cleared them; this
 *	may be another backend or a restarted instance), so everything is
 *	driven from the 2PC record.
 *
 *	Commit: per-binding durable 0x30 commit (same primitive as the
 *	single-machine pre-commit hook, spec-3.11 C1/C10: no independent
 *	fsync -- the prepared-commit WAL flush carries the 0x30 record) plus
 *	the COMMITTED overlay install + cross-node hint.  Sub-links need no
 *	explicit resolve: the spec-3.5 lazy parent-follow reads the parent's
 *	terminal state.
 *
 *	Abort: per-binding durable 0x60 abort-clear (step 7) plus the
 *	ABORTED overlay install + hint.
 *
 *	Failure anywhere here is safe (C-P6): the transaction is still
 *	prepared and the command can be retried.
 */
void
cluster_tt_twophase_prefinish(TransactionId xid, SCN final_scn, bool is_commit, const void *recdata,
							  uint32 len)
{
	ClusterTT2PCParsed p;
	uint16 i;

	Assert(cluster_node_id >= 0);

	parse_or_corrupt(xid, recdata, len, &p);

	if (is_commit)
		cluster_vis_bump_twopc_prefinish_commits();
	else
		cluster_vis_bump_twopc_prefinish_aborts();

	for (i = 0; i < p.nbindings; i++) {
		const ClusterTT2PCBinding *b = &p.bindings[i];
		ClusterTTStatusKey key;

		memset(&key, 0, sizeof(key));
		key.origin_node_id = (uint16)cluster_node_id;
		key.undo_segment_id = (uint16)b->undo_segment_id;
		key.tt_slot_id = cluster_tt_slot_offset_to_id(b->slot_offset);
		key.cluster_epoch = b->cluster_epoch;
		key.local_xid = b->xid;

		if (is_commit) {
			cluster_tt_slot_durable_commit(b->undo_segment_id, b->slot_offset, b->xid, b->wrap,
										   final_scn);
			cluster_tt_slot_mark_committed(b->undo_segment_id, b->slot_offset, b->xid, final_scn);
			(void)cluster_tt_status_install_local(&key, CLUSTER_TT_STATUS_COMMITTED, final_scn);
			cluster_tt_status_hint_emit(&key, CLUSTER_TT_STATUS_COMMITTED, final_scn);
		} else {
			cluster_tt_slot_durable_abort(b->undo_segment_id, b->slot_offset, b->xid, b->wrap);
			/*
			 * spec-4.8 D7-A: persist this binding's undo-chain head (captured
			 * into the v2 2PC record at PREPARE) durably onto the now-ABORTED
			 * slot, so D7 physical rollback can walk the chain after a crash-
			 * restart.  Emit order matters: durable_abort above stamped the
			 * slot's xid/wrap, so set_head's identity gate matches at redo.  A
			 * v1 record / a binding with no undo (heads NULL or InvalidUba) is a
			 * no-op -> D7 fails closed for it (MVCC invisible + vacuum, I10).
			 */
			if (p.heads != NULL && !UBA_is_invalid(p.heads[i]))
				cluster_tt_slot_durable_set_head(b->undo_segment_id, b->slot_offset, b->xid,
												 b->wrap, p.heads[i]);
			cluster_tt_slot_mark_aborted(b->undo_segment_id, b->slot_offset, b->xid);
			(void)cluster_tt_status_install_local(&key, CLUSTER_TT_STATUS_ABORTED, InvalidScn);
			cluster_tt_status_hint_emit(&key, CLUSTER_TT_STATUS_ABORTED, InvalidScn);
		}
	}
}

#endif /* USE_PGRAC_CLUSTER */
