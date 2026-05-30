/*-------------------------------------------------------------------------
 *
 * cluster_cr_apply.c
 *	  pgrac CR inverse-apply helpers (spec-3.9 D4).
 *
 *	  Four pure-logic helpers that inverse-apply one undo record onto a
 *	  backend-local CR scratch page.  Driven by the chain walker in
 *	  cluster_cr.c.  See cluster_cr_apply.h for the per-helper contract.
 *
 *	  NOTE (spec-3.9 Step 3 commit): the four bodies are explicit
 *	  FEATURE_NOT_SUPPORTED stubs.  The real inverse mutations land in
 *	  Step 4 (D4).  This keeps the Step 3 commit linkable while honoring
 *	  CLAUDE.md 规则 8 (no fake-success stub) — a CR chain that reaches an
 *	  INSERT/UPDATE/DELETE/ITL record before Step 4 lands raises a clear
 *	  "not yet implemented" error rather than silently returning a wrong
 *	  image.  Nothing calls the chain walker until Step 5 (visibility
 *	  integration), which lands after Step 4.
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
 *	  src/backend/cluster/cluster_cr_apply.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/elog.h"

#include "cluster/cluster_cr_apply.h"


static void
cr_apply_not_implemented(const char *which)
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster CR %s inverse-apply not yet implemented", which),
					errhint("Lands in spec-3.9 Step 4 (D4); unreachable until Step 5 "
							"visibility integration.")));
}

bool
cluster_cr_apply_insert_inverse(char *scratch_page, const UndoRecordHeader *hdr,
								const UndoInsertPayload *payload)
{
	(void)scratch_page;
	(void)hdr;
	(void)payload;
	cr_apply_not_implemented("insert");
	return false; /* unreachable */
}

bool
cluster_cr_apply_update_inverse(char *scratch_page, const UndoRecordHeader *hdr,
								const UndoUpdatePayload *payload, const char *old_tuple_bytes,
								uint16 old_tuple_length)
{
	(void)scratch_page;
	(void)hdr;
	(void)payload;
	(void)old_tuple_bytes;
	(void)old_tuple_length;
	cr_apply_not_implemented("update");
	return false; /* unreachable */
}

bool
cluster_cr_apply_delete_inverse(char *scratch_page, const UndoRecordHeader *hdr,
								const UndoDeletePayload *payload, const char *old_tuple_bytes,
								uint16 old_tuple_length)
{
	(void)scratch_page;
	(void)hdr;
	(void)payload;
	(void)old_tuple_bytes;
	(void)old_tuple_length;
	cr_apply_not_implemented("delete");
	return false; /* unreachable */
}

bool
cluster_cr_apply_itl_inverse(char *scratch_page, const UndoRecordHeader *hdr, int itl_idx)
{
	(void)scratch_page;
	(void)hdr;
	(void)itl_idx;
	cr_apply_not_implemented("ITL");
	return false; /* unreachable */
}
