/*-------------------------------------------------------------------------
 *
 * cluster_cr_apply.h
 *	  pgrac CR inverse-apply helpers (spec-3.9 D4).
 *
 *	  Four pure-logic helpers that inverse-apply one undo record onto a
 *	  backend-local CR scratch page.  Exposed via this header so the
 *	  cluster_unit harness can unit-test each helper in isolation against
 *	  synthetic scratch buffers + payloads.
 *
 *	  Inverse-apply contract:
 *	    - scratch_page : BLCKSZ-byte mutable page (already memcpy'd from the
 *	                     original buffer); the helper mutates it in place.
 *	    - hdr          : UndoRecordHeader read from the undo segment; the
 *	                     chain walker has already validated CRC + that the
 *	                     record_type matches this helper.
 *	    - payload      : the typed payload struct (resolved by record_type).
 *	    - old_tuple_*  : for update/delete inverse, the pointer + length of
 *	                     the old/full tuple image bytes the chain walker
 *	                     resolved from payload->{old_tuple_offset,
 *	                     full_tuple_offset} within the undo record buffer.
 *
 *	    Returns true on success; false on corruption (caller ereports
 *	    data_corrupted — spec-3.9 I-fail-4: the caller MUST check the bool,
 *	    a (void) cast is forbidden).
 *
 *	  Mutations:
 *	    - insert_inverse : remove the inserted tuple (LP_UNUSED + pd_lower
 *	                       rewind if it was the last line pointer)
 *	    - update_inverse : overwrite the new tuple bytes with the old tuple
 *	                       image from the undo record
 *	    - delete_inverse : restore the deleted tuple's visibility (xmax ->
 *	                       InvalidTransactionId + clear HEAP_XMAX_* flags),
 *	                       restoring the full tuple image if needed
 *	    - itl_inverse    : restore the on-page ITL slot metadata (uba_head /
 *	                       write_scn) to its pre-apply state so the chain
 *	                       walker unwinds a reused ITL slot correctly
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-3.9-own-instance-cr-block-construction.md (FROZEN v0.4)
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_cr_apply.h
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *	  Bodies land in src/backend/cluster/cluster_cr_apply.c (Step 4);
 *	  this header declares the schema only (spec-3.9 Step 1 / D1).
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CR_APPLY_H
#define CLUSTER_CR_APPLY_H

#ifndef FRONTEND

#include "postgres.h"

#include "cluster/cluster_itl_slot.h"	 /* ClusterItlSlotData (spec-3.10 candidate collect) */
#include "cluster/cluster_scn.h"		 /* SCN (spec-3.10) */
#include "cluster/cluster_uba.h"		 /* UBA (spec-3.10) */
#include "cluster/cluster_undo_record.h" /* UndoRecordHeader + 3 payloads */
#include "storage/off.h"


/*
 * spec-3.10 D1: candidate ITL chain captured from a CR scratch page BEFORE any
 * inverse-apply.  Full-block CR collects the candidate chains up front because
 * cluster_cr_apply_itl_inverse mutates the on-page ITL slots; re-reading chain
 * heads from the mutating page would corrupt chain selection.  Chains are then
 * inverse-applied newest-transaction-first (write_scn DESC, spec-3.10 Q10): the
 * prev_uba chain is per-transaction and each inverse is an unconditional
 * old-image restore, so a row touched by txB then txC must be peeled C -> B.
 */
typedef struct ClusterCRCandidateChain {
	uint8 slot_idx;		   /* source ITL slot (identity / diag) */
	TransactionId xid;	   /* slot xid (post-construct xmin prune) */
	SCN write_scn;		   /* slot write_scn (sort key) */
	UBA undo_segment_head; /* captured chain head */
} ClusterCRCandidateChain;

/*
 * NB: no origin_node_id field — cross-instance detection is authoritative
 * per-undo-record in the chain walker (hdr->origin_node_id -> 53R9G); pulling
 * uba_origin_node_id() into this otherwise-pure helper would add an extern link
 * dependency the cluster_unit harness does not need (spec-3.10 v0.3 impl note).
 */

/*
 * cluster_cr_collect_candidate_chains -- snapshot every ITL slot whose
 *   write_scn is newer than read_scn into out[] (capacity max_out, normally
 *   CLUSTER_ITL_INITRANS_DEFAULT).  Returns the number captured.  Pure: reads
 *   the slot array only, never the undo storage; safe to call before mutation.
 */
extern int cluster_cr_collect_candidate_chains(const ClusterItlSlotData *slots, SCN read_scn,
											   ClusterCRCandidateChain *out, int max_out);

/*
 * cluster_cr_chain_cmp_by_write_scn_desc -- qsort comparator ordering
 *   ClusterCRCandidateChain by write_scn DESCENDING (newest transaction first).
 */
extern int cluster_cr_chain_cmp_by_write_scn_desc(const void *a, const void *b);

/*
 * cluster_cr_prune_post_snapshot_versions -- after a full-block CR has replayed
 *   every candidate chain, remove NEW tuple versions created after read_scn.
 *
 *   Reverting a chain clears a tuple's xmax but does NOT remove the new physical
 *   version an UPDATE produced: pgrac's heap_update emits only an UNDO_RECORD_
 *   UPDATE (reverts the OLD tuple's header), never an INSERT-undo for the new
 *   version (verified at heapam.c heap_update).  A tuple whose xmin is one of
 *   the candidate (post-read_scn) transactions did not exist at the snapshot, so
 *   we mark it LP_UNUSED.  This is what makes the same-row-multi-update case
 *   correct (the per-tuple gate xmin check cannot, because a doubly-updated
 *   tuple's LIVE slot is its latest modifier, not its creator — spec-3.10 L4b).
 *
 *   Returns the number of tuples pruned.  Pure: page + candidate array only.
 */
extern int cluster_cr_prune_post_snapshot_versions(char *scratch_page,
												   const ClusterCRCandidateChain *chains,
												   int nchains);


extern bool cluster_cr_apply_insert_inverse(char *scratch_page, const UndoRecordHeader *hdr,
											const UndoInsertPayload *payload);

extern bool cluster_cr_apply_update_inverse(char *scratch_page, const UndoRecordHeader *hdr,
											const UndoUpdatePayload *payload,
											const char *old_tuple_bytes, uint16 old_tuple_length);

extern bool cluster_cr_apply_delete_inverse(char *scratch_page, const UndoRecordHeader *hdr,
											const UndoDeletePayload *payload,
											const char *old_tuple_bytes, uint16 old_tuple_length);

/*
 * spec-3.9 Step 4 signature correction (flag for user codereview):
 *   the spec §2.2 sketch declared itl_inverse(scratch_page, hdr, int itl_idx),
 *   but restoring the lock-only ITL transition needs the captured prior state
 *   in UndoItlPayload (prev tuple header fields + prev ITL slot fields incl.
 *   the slot's own itl_slot_idx).  The chain-root itl_idx is insufficient, so
 *   the real signature takes the payload.  Verify-linkdb-first against
 *   UndoItlPayload (cluster_undo_record.h) drove this.
 */
extern bool cluster_cr_apply_itl_inverse(char *scratch_page, const UndoRecordHeader *hdr,
										 const UndoItlPayload *payload);

#endif /* !FRONTEND */

#endif /* CLUSTER_CR_APPLY_H */
