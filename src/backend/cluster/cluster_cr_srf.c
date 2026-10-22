/*-------------------------------------------------------------------------
 *
 * cluster_cr_srf.c
 *	  pgrac TEST-ONLY SQL entry points to drive CR block construction
 *	  deterministically from cluster_tap / cluster_unit.
 *
 *	  cluster_cr_test_construct(rel regclass, blockno int4, itl_idx int4,
 *	                            read_scn int8) -> bool            (spec-3.9 D10)
 *
 *	  Reads block `blockno` of `rel` under a SHARE content lock and calls
 *	  cluster_cr_construct_block(buf, read_scn, itl_idx).  Returns true on a
 *	  successful construction; raises the CR taxonomy SQLSTATE (53R9F /
 *	  53R9G / data_corrupted) when a cr_* injection point is armed or the
 *	  undo chain is genuinely unreconstructable.  Superuser-only.
 *
 *	  cluster_cr_test_image(rel regclass, blockno int4, read_scn int8)
 *	                            -> SETOF record                  (spec-3.10 §v0.6)
 *
 *	  Constructs the CR image of `blockno` as-of `read_scn` and returns one
 *	  row per NORMAL line pointer on the reconstructed image: a leading
 *	  cr_off (int2 = the offnum, so callers can assert ctid/offset stability)
 *	  followed by the relation's own columns.  The caller supplies the column
 *	  definition list, e.g.
 *	      SELECT * FROM cluster_cr_test_image('t'::regclass, 0, $scn)
 *	             AS r(cr_off int2, id int, v text);
 *	  This lets t/218 assert CR-image CONTENT (the read_scn state is rebuilt,
 *	  post-read_scn versions do not appear), not merely that construction did
 *	  not error (spec-3.10 §v0.6 §L3).  Superuser-only.
 *
 *	  cluster_cr_test_target_image(rel regclass, blockno int4, read_scn int8)
 *	                            -> SETOF record
 *
 *	  Returns the same raw row shape through the existing target FULL-block
 *	  API. Only FULL/NONE is consumable; refusal or deferral raises an error
 *	  without a local buffer read, own-instance fallback or retry. Accepts
 *	  ordinary heap tables and heap materialized views. Superuser-only.
 *
 *	  These bypass ordinary scan setup so the CR read path is exercisable
 *	  deterministically from TAP and cluster_unit fixtures.  Mirrors spec-3.8's
 *	  cluster_undo_test_force_segment_end.
 *
 *	  TEST-ONLY: these are diagnostic entry points, NOT product query
 *	  interfaces; they expose raw reconstructed block state and are
 *	  superuser-only (spec-3.10 §v0.6 §L2).
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
 *	  src/backend/cluster/cluster_cr_srf.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

PG_FUNCTION_INFO_V1(cluster_cr_test_construct);
PG_FUNCTION_INFO_V1(cluster_cr_test_image);
PG_FUNCTION_INFO_V1(cluster_cr_test_target_image);

#ifdef USE_PGRAC_CLUSTER

#include "access/htup_details.h"
#include "access/relation.h"
#include "catalog/pg_am_d.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "storage/buf_internals.h"
#include "storage/bufmgr.h"
#include "storage/bufpage.h"
#include "storage/itemptr.h"
#include "utils/rel.h"
#include "utils/relcache.h"

#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_scn.h"

Datum
cluster_cr_test_construct(PG_FUNCTION_ARGS)
{
	Oid relid;
	int32 blockno;
	int32 itl_idx;
	int64 read_scn_in;
	Relation rel;
	Buffer buf;
	const char *cr_page
		= NULL; /* init: assigned in PG_TRY, PG_CATCH RE_THROWs (cppcheck longjmp blind spot) */
	SCN read_scn;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_cr_test_construct is superuser-only")));

	relid = PG_GETARG_OID(0);
	blockno = PG_GETARG_INT32(1);
	itl_idx = PG_GETARG_INT32(2); /* spec-3.10 D7: retained for ABI but ignored
								   * (full-block CR rolls back every chain) */
	(void)itl_idx;
	read_scn_in = PG_GETARG_INT64(3);
	read_scn = (SCN)read_scn_in;

	rel = relation_open(relid, AccessShareLock);

	buf = ReadBuffer(rel, (BlockNumber)blockno);
	LockBuffer(buf, BUFFER_LOCK_SHARE);

	PG_TRY();
	{
		/* construct_block ereports the CR taxonomy SQLSTATE on failure /
		 * armed injection; on success returns the scratch image pointer. */
		cr_page = cluster_cr_construct_block(buf, read_scn);
	}
	PG_CATCH();
	{
		/*
		 * PG_CATCH restores the pre-try interrupt holdoff count, but the
		 * buffer content lock is still held.  Match LWLockReleaseAll()'s error
		 * cleanup pattern so LockBuffer(...UNLOCK)'s internal RESUME_INTERRUPTS
		 * does not underflow in cassert builds.
		 */
		HOLD_INTERRUPTS();
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buf);
		relation_close(rel, AccessShareLock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	LockBuffer(buf, BUFFER_LOCK_UNLOCK);
	ReleaseBuffer(buf);
	relation_close(rel, AccessShareLock);

	PG_RETURN_BOOL(cr_page != NULL);
}


/*
 * cluster_cr_test_image -- TEST-ONLY: construct the CR image of one block and
 *	return its NORMAL tuples as rows (leading cr_off int2 + the rel's columns).
 *	See the file banner + spec-3.10 §v0.6 §L1/§L2 for the contract.
 *
 *	The CR image is the read_scn block state: post-read_scn versions are pruned
 *	and pre-change images restored, so the NORMAL line pointers on the image are
 *	exactly the tuples physically present at read_scn (the test scenarios avoid
 *	pre-read_scn-deleted tuples, whose committed xmax this raw view would not
 *	re-hide — the MVCC gate, not this diagnostic SRF, enforces full visibility).
 */
static Datum
cr_test_image_rows(FunctionCallInfo fcinfo, bool target_full)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *)fcinfo->resultinfo;
	Oid relid;
	int32 blockno;
	int64 read_scn_in;
	SCN read_scn;
	Relation rel;
	TupleDesc rel_desc;
	TupleDesc out_desc;
	int nrelatts;
	Buffer buf = InvalidBuffer;
	Page page;
	OffsetNumber off;
	OffsetNumber maxoff;
	int i;
	char *image; /* private copy of the reconstructed CR image */
	Datum *out_values;
	bool *out_nulls;
	Datum *rel_values;
	bool *rel_nulls;
	const char *entry_name = target_full ? "cluster_cr_test_target_image" : "cluster_cr_test_image";

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("%s is superuser-only", entry_name)));

	relid = PG_GETARG_OID(0);
	blockno = PG_GETARG_INT32(1);
	read_scn_in = PG_GETARG_INT64(2);
	read_scn = (SCN)read_scn_in;

	/* Materialize-mode SRF: sets rsinfo->setResult / setDesc from the caller's
	 * column definition list (the AS r(cr_off int2, ...) clause). */
	InitMaterializedSRF(fcinfo, 0);
	out_desc = rsinfo->setDesc;

	rel = relation_open(relid, AccessShareLock);
	rel_desc = RelationGetDescr(rel);
	nrelatts = rel_desc->natts;

	/* The target image is a heap page; do not deform another AM's tuples. */
	if (target_full
		&& ((rel->rd_rel->relkind != RELKIND_RELATION && rel->rd_rel->relkind != RELKIND_MATVIEW)
			|| rel->rd_rel->relam != HEAP_TABLE_AM_OID)) {
		relation_close(rel, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
						errmsg("%s requires a heap table or materialized view", entry_name),
						errhint("Use an ordinary heap table or heap materialized view.")));
	}

	if (out_desc->natts != nrelatts + 1) {
		relation_close(rel, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("%s column definition must be "
							   "cr_off plus the relation's %d column(s)",
							   entry_name, nrelatts),
						errhint("e.g. AS r(cr_off int2, <relation columns...>)")));
	}
	if (TupleDescAttr(out_desc, 0)->atttypid != INT2OID) {
		relation_close(rel, AccessShareLock);
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("%s first output column must be int2", entry_name),
						errhint("e.g. AS r(cr_off int2, <relation columns...>)")));
	}
	for (i = 0; i < nrelatts; i++) {
		Form_pg_attribute rel_att = TupleDescAttr(rel_desc, i);
		Form_pg_attribute out_att = TupleDescAttr(out_desc, i + 1);

		if (rel_att->attisdropped)
			continue;

		if (out_att->atttypid != rel_att->atttypid || out_att->atttypmod != rel_att->atttypmod) {
			relation_close(rel, AccessShareLock);
			ereport(
				ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("%s output column %d must match relation column type", entry_name, i + 2),
				 errhint("declare column %s with type OID %u and typmod %d",
						 NameStr(rel_att->attname), rel_att->atttypid, rel_att->atttypmod)));
		}
	}

	image = (char *)palloc(BLCKSZ);

	if (!target_full) {
		buf = ReadBuffer(rel, (BlockNumber)blockno);
		LockBuffer(buf, BUFFER_LOCK_SHARE);
	}

	PG_TRY();
	{
		if (target_full) {
			BufferTag tag;
			ClusterCrBuildResult result;
			ClusterCrBuildReason reason = CLUSTER_CR_BUILD_PROTOCOL;

			InitBufferTag(&tag, &rel->rd_locator, MAIN_FORKNUM, (BlockNumber)blockno);
			result = cluster_gcs_block_cr_fetch_and_wait(tag, read_scn, image, &reason);
			if (result != CLUSTER_CR_BUILD_FULL || reason != CLUSTER_CR_BUILD_NONE)
				ereport(ERROR,
						(errcode(ERRCODE_CLUSTER_TT_STATUS_UNKNOWN),
						 errmsg("target CR test did not return a complete page"),
						 errdetail("result=%d reason=%d; only FULL/NONE is consumable",
								   (int)result, (int)reason)));
		} else {
			const char *cr_page = cluster_cr_construct_block(buf, read_scn);

			/* Copy out under the content lock; cr_page is the backend-local scratch
			 * (valid only until the next construct), so snapshot it now. */
			memcpy(image, cr_page, BLCKSZ);
		}
	}
	PG_CATCH();
	{
		if (BufferIsValid(buf)) {
			HOLD_INTERRUPTS();
			LockBuffer(buf, BUFFER_LOCK_UNLOCK);
			ReleaseBuffer(buf);
		}
		relation_close(rel, AccessShareLock);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (BufferIsValid(buf)) {
		LockBuffer(buf, BUFFER_LOCK_UNLOCK);
		ReleaseBuffer(buf);
	}

	out_values = (Datum *)palloc(sizeof(Datum) * out_desc->natts);
	out_nulls = (bool *)palloc(sizeof(bool) * out_desc->natts);
	rel_values = (Datum *)palloc(sizeof(Datum) * nrelatts);
	rel_nulls = (bool *)palloc(sizeof(bool) * nrelatts);

	page = (Page)image;
	maxoff = PageGetMaxOffsetNumber(page);
	for (off = FirstOffsetNumber; off <= maxoff; off++) {
		ItemId iid = PageGetItemId(page, off);
		HeapTupleData tup;
		int attr_index;

		if (!ItemIdIsNormal(iid))
			continue;

		tup.t_len = ItemIdGetLength(iid);
		tup.t_data = (HeapTupleHeader)PageGetItem(page, iid);
		ItemPointerSet(&tup.t_self, (BlockNumber)blockno, off);
		tup.t_tableOid = relid;

		heap_deform_tuple(&tup, rel_desc, rel_values, rel_nulls);

		out_values[0] = Int16GetDatum((int16)off);
		out_nulls[0] = false;
		for (attr_index = 0; attr_index < nrelatts; attr_index++) {
			out_values[attr_index + 1] = rel_values[attr_index];
			out_nulls[attr_index + 1] = rel_nulls[attr_index];
		}
		/* putvalues copies the data, so it survives image/rel close. */
		tuplestore_putvalues(rsinfo->setResult, out_desc, out_values, out_nulls);
	}

	relation_close(rel, AccessShareLock);
	return (Datum)0;
}

Datum
cluster_cr_test_image(PG_FUNCTION_ARGS)
{
	return cr_test_image_rows(fcinfo, false);
}

Datum
cluster_cr_test_target_image(PG_FUNCTION_ARGS)
{
	return cr_test_image_rows(fcinfo, true);
}

#else /* !USE_PGRAC_CLUSTER */

Datum
cluster_cr_test_construct(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_cr_test_construct requires --enable-cluster")));
	PG_RETURN_NULL();
}

Datum
cluster_cr_test_image(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_cr_test_image requires --enable-cluster")));
	PG_RETURN_NULL();
}

Datum
cluster_cr_test_target_image(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_cr_test_target_image requires --enable-cluster"),
					errhint("Rebuild with --enable-cluster to use this diagnostic entry.")));
	PG_RETURN_NULL();
}

#endif /* USE_PGRAC_CLUSTER */
