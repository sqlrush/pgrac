/*-------------------------------------------------------------------------
 *
 * cluster_tt_2pc.h
 *	  pgrac cluster TT state in two-phase commit (spec-3.15).
 *
 *	  PREPARE TRANSACTION serializes the transaction's cluster TT
 *	  bindings (xid -> undo segment / slot / wrap / epoch) plus its
 *	  SUBCOMMITTED sub-links into a 2PC record carried by PostgreSQL's
 *	  twophase state file (TWOPHASE_RM_CLUSTER_TT_ID).  COMMIT/ROLLBACK
 *	  PREPARED resolve the durable TT through that record (the backend
 *	  -local binding array is gone by then -- another backend, or a
 *	  restarted instance, performs the resolve).  Crash recovery re-pins
 *	  the slots via a protected-slot map so the allocator cannot hand
 *	  them to new transactions (V-4).
 *
 *	  Ordering contract (C-P6): durable TT resolve (0x30 commit / 0x60
 *	  abort-clear) happens in FinishPreparedTransaction BEFORE the
 *	  prepared commit/abort WAL record -- the twophase post callbacks
 *	  run too late and carry no final_scn (C-P7), so they only do
 *	  non-fallible cleanup.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_tt_2pc.h
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-3.15-2pc-prepared-visibility.md (FROZEN v0.2) §2.1.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_TT_2PC_H
#define CLUSTER_TT_2PC_H

#include "c.h"
#include "access/transam.h"

#include "cluster/cluster_itl_slot.h" /* UBA (spec-4.8 D7-A undo head) */
#include "cluster/cluster_scn.h"
#include "cluster/cluster_tt_status.h" /* ClusterTTStatusKey */


#define CLUSTER_TT_2PC_MAGIC 0x50324354 /* "P2CT" */
/*
 * Version 1 (spec-3.15): [hdr][bindings 16B][sublinks].
 * Version 2 (spec-4.8 D7-A): appends a heads[] section parallel to bindings[]
 *	-- heads[i] is binding[i]'s latest undo-chain head (TTSlot.first_undo_block)
 *	captured at PREPARE while it is still in backend-local memory.  The binding
 *	struct is UNCHANGED (16B, zero-copy) so a v1 record still parses (its
 *	heads[] is absent -> NULL -> D7 physical rollback is a no-op for it).
 */
#define CLUSTER_TT_2PC_VERSION 2
#define CLUSTER_TT_2PC_VERSION_NO_HEADS 1 /* legacy: bindings only, no heads[] */
#define CLUSTER_TT_2PC_MAX_BINDINGS 64	  /* §1.4-4: xact-local TT bindings cap */
#define CLUSTER_TT_2PC_MAX_SUBLINKS 64	  /* SUBCOMMITTED links cap (R7) */

/*
 * One TT binding of the prepared transaction.  origin is implicitly the
 * local instance (a prepared xact is always resolved on its own node at
 * Stage 3; cross-instance coordination is feature #70 / Stage 5).
 * 16 bytes, no implicit padding.
 */
typedef struct ClusterTT2PCBinding {
	uint32 undo_segment_id;
	uint16 slot_offset;
	uint16 wrap;
	uint32 cluster_epoch;
	TransactionId xid;
} ClusterTT2PCBinding;

StaticAssertDecl(sizeof(ClusterTT2PCBinding) == 16,
				 "spec-3.15: ClusterTT2PCBinding is 16 bytes, no implicit padding");

/*
 * One SUBCOMMITTED link (spec-3.5 chain node) of the prepared xact.
 * Full exact keys on both sides (a bare 32-bit subxid would be
 * epoch/origin/wrap-ambiguous -- HC180 family).
 */
typedef struct ClusterTT2PCSubLink {
	ClusterTTStatusKey child_key;
	ClusterTTStatusKey parent_key;
} ClusterTT2PCSubLink;

/*
 * Record header.  Followed in the serialized payload by
 * bindings[nbindings] then sublinks[nsublinks]; crc covers everything
 * after the crc field itself.
 */
typedef struct ClusterTT2PCRecord {
	uint32 magic;
	uint16 version;
	uint16 nbindings;
	uint32 nsublinks;
	uint32 crc;
} ClusterTT2PCRecord;

StaticAssertDecl(sizeof(ClusterTT2PCRecord) == 16,
				 "spec-3.15: ClusterTT2PCRecord header is 16 bytes");

/*
 * Parsed view over a validated record (pointers into the caller's
 * buffer; no copies).
 */
typedef struct ClusterTT2PCParsed {
	uint16 version;
	uint16 nbindings;
	uint32 nsublinks;
	const ClusterTT2PCBinding *bindings;
	const ClusterTT2PCSubLink *sublinks;
	/*
	 * spec-4.8 D7-A: heads[i] is bindings[i]'s undo-chain head, present iff
	 * version >= CLUSTER_TT_2PC_VERSION (2) AND nbindings > 0; else NULL (a v1
	 * legacy record -> no heads -> D7 physical rollback no-op for it).  A
	 * heads[i] of InvalidUba means "no recorded head for this binding".
	 */
	const UBA *heads;
} ClusterTT2PCParsed;


/*
 * Pure serialize / parse layer (cluster_unit-tested; the AtPrepare /
 * callback shells stay thin -- L212).
 *
 * build: returns a palloc'd buffer + its length; nbindings/nsublinks
 * already validated against the caps by the caller-facing shell.
 * parse: validates magic/version/length/crc and fills `out` with
 * pointers into `recdata`; returns false on any mismatch (caller
 * fail-closes with DATA_CORRUPTED).
 */
/* Exact serialized size for a given format version + counts (header + payload).
 * version 1 = bindings + sublinks; version 2 = + heads[nbindings]. */
extern uint32 cluster_tt_2pc_record_size(uint16 version, uint16 nbindings, uint32 nsublinks);
/* Serialize a version-2 record into a caller-provided buffer (dstcap >=
 * record_size(2,...)); returns bytes written, 0 on cap/limit violation.  heads
 * (parallel to bindings; may be NULL -> all InvalidUba) carries each binding's
 * undo-chain head.  Pure: no allocation. */
extern uint32 cluster_tt_2pc_serialize(const ClusterTT2PCBinding *bindings, const UBA *heads,
									   uint16 nbindings, const ClusterTT2PCSubLink *sublinks,
									   uint32 nsublinks, char *dst, uint32 dstcap);
extern bool cluster_tt_2pc_parse_record(const void *recdata, uint32 len, ClusterTT2PCParsed *out);

/*
 * PREPARE-side shells (wired into xact.c's AtPrepare_* / PostPrepare_*
 * chains at step 4).  AtPrepare serializes only -- NO state mutation
 * (PG two-phase contract; an EndPrepare failure must still abort with
 * the backend-local state intact).  PostPrepare transfers ownership to
 * the 2PC record: clears the backend-local TT bindings, sub-links and
 * the ITL touch list (V-2: the touch list is droppable -- overlay /
 * durable TT are authoritative and 3.4c lazy cleanout re-stamps pages).
 */
extern void AtPrepare_ClusterTT(void);
extern void PostPrepare_ClusterTT(void);

/*
 * FinishPreparedTransaction prefinish (C-P6): called AFTER final_scn is
 * produced and BEFORE RecordTransactionCommitPrepared/AbortPrepared.
 * Commit: per-binding 0x30 durable commit + overlay COMMITTED.
 * Abort:  per-binding 0x60 durable abort-clear + overlay ABORTED.
 * Failure here is safe: the xact is still prepared and retryable.
 */
extern void cluster_tt_twophase_prefinish(TransactionId xid, SCN final_scn, bool is_commit,
										  const void *recdata, uint32 len);

/*
 * twophase_rmgr callbacks (registered at step 3).  recover re-pins the
 * prepared slots into the protected-slot map (V-4) and rebuilds the
 * SUBCOMMITTED overlay; postcommit/postabort do non-fallible cleanup
 * only (C-P7).
 */
extern void cluster_tt_twophase_recover(TransactionId xid, uint16 info, void *recdata, uint32 len);
/* spec-3.16 D1: standby overlay rebuild (cluster-only traversal; no protected map). */
extern void cluster_tt_twophase_standby_recover(TransactionId xid, uint16 info, void *recdata,
												uint32 len);
extern int cluster_tt_twophase_standby_commit_prepared(TransactionId xid, SCN commit_scn);
extern void cluster_tt_twophase_postcommit(TransactionId xid, uint16 info, void *recdata,
										   uint32 len);
extern void cluster_tt_twophase_postabort(TransactionId xid, uint16 info, void *recdata,
										  uint32 len);

#endif /* CLUSTER_TT_2PC_H */
