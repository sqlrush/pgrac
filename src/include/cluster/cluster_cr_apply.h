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

#include "cluster/cluster_undo_record.h" /* UndoRecordHeader + 3 payloads */
#include "storage/off.h"


extern bool cluster_cr_apply_insert_inverse(char *scratch_page,
											const UndoRecordHeader *hdr,
											const UndoInsertPayload *payload);

extern bool cluster_cr_apply_update_inverse(char *scratch_page,
											const UndoRecordHeader *hdr,
											const UndoUpdatePayload *payload,
											const char *old_tuple_bytes,
											uint16 old_tuple_length);

extern bool cluster_cr_apply_delete_inverse(char *scratch_page,
											const UndoRecordHeader *hdr,
											const UndoDeletePayload *payload,
											const char *old_tuple_bytes,
											uint16 old_tuple_length);

extern bool cluster_cr_apply_itl_inverse(char *scratch_page,
										 const UndoRecordHeader *hdr,
										 int itl_idx);

#endif /* !FRONTEND */

#endif /* CLUSTER_CR_APPLY_H */
