/*-------------------------------------------------------------------------
 *
 * cluster_reverse_key.c
 *	  pgrac spec-6.12f -- reverse-key btree index encoding.
 *
 *	  See cluster_reverse_key.h for the contract.  The transform is a pure
 *	  byte-reversal of a fixed-width pass-by-value integer datum; the
 *	  nbtree AM applies it at insert and equality-search so the stored key
 *	  and the search key are reversed consistently.  A build-time validator
 *	  rejects unsupported index shapes so there is never a silent
 *	  wrong-result path (rule 8).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_reverse_key.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/nbtree.h"
#include "catalog/pg_type_d.h"
#include "cluster/cluster_reverse_key.h"
#include "fmgr.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

bool
cluster_reverse_key_typlen_supported(int16 typlen)
{
	return typlen == 2 || typlen == 4 || typlen == 8;
}

Datum
cluster_reverse_key_encode(Datum value, int16 typlen)
{
	uint64 v;
	uint64 r = 0;
	int i;

	/*
	 * Pull the low `typlen` bytes out of the Datum, byte-reverse them, and
	 * repack.  Datum stores an integer of width typlen in its low bytes on
	 * every supported platform (int2/int4/int8 are pass-by-value).
	 */
	switch (typlen) {
	case 2:
		v = (uint64)DatumGetUInt16(value);
		break;
	case 4:
		v = (uint64)DatumGetUInt32(value);
		break;
	case 8:
		v = (uint64)DatumGetUInt64(value);
		break;
	default:
		/* Unsupported; validated away at CREATE INDEX.  Fail loud in
			 * assert builds, pass through in production (never reached). */
		Assert(false);
		return value;
	}

	for (i = 0; i < typlen; i++) {
		r = (r << 8) | (v & 0xFF);
		v >>= 8;
	}

	switch (typlen) {
	case 2:
		return UInt16GetDatum((uint16)r);
	case 4:
		return UInt32GetDatum((uint32)r);
	default:
		return UInt64GetDatum(r);
	}
}

void
cluster_reverse_key_validate_index(Relation index)
{
	Form_pg_attribute att;

	if (!BTGetClusterReverseKey(index))
		return;

	if (IndexRelationGetNumberOfKeyAttributes(index) != 1)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster_reverse_key requires a single-column index"),
						errhint("Reverse-key indexes scatter one leading integer key; "
								"build a separate plain index for other columns.")));

	att = TupleDescAttr(RelationGetDescr(index), 0);

	/*
	 * Pin the key to exactly int2 / int4 / int8, not merely "byval of a
	 * supported width": other fixed-width byval types (oid, timestamp,
	 * date, ...) either sit in opfamilies with cross-type operators whose
	 * semantics are not integer widening (timestamp vs date) or add no
	 * value, and the scankey cross-type normalization
	 * (cluster_reverse_key_transform_scankeys) assumes integer
	 * sign-extension.
	 */
	if (att->atttypid != INT2OID && att->atttypid != INT4OID && att->atttypid != INT8OID)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cluster_reverse_key supports only smallint / integer / bigint keys"),
				 errhint("Byte-reversal is defined for fixed-width pass-by-value "
						 "integer keys (int2 / int4 / int8).")));

	Assert(att->attbyval && cluster_reverse_key_typlen_supported(att->attlen));
}

/*
 * Extract an integer probe datum of type `subtype` into an int64, then
 * repack it as the index key type (`keytype`).  Widening sign-extends;
 * narrowing checks the range and reports failure (the probe value provably
 * matches no stored key) by returning false.  Only the three integer types
 * can appear: the CREATE-INDEX validator pinned the key type, and
 * integer_ops has cross-type operators only among int2/int4/int8.
 */
static bool
reverse_key_normalize_probe(Oid subtype, Datum probe, Oid keytype, Datum *normalized)
{
	int64 v;

	switch (subtype) {
	case INT2OID:
		v = (int64)DatumGetInt16(probe);
		break;
	case INT4OID:
		v = (int64)DatumGetInt32(probe);
		break;
	case INT8OID:
		v = DatumGetInt64(probe);
		break;
	default:
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("cluster_reverse_key index probed with unexpected type %u", subtype),
						errhint("Only smallint / integer / bigint equality probes are "
								"supported on a reverse-key index.")));
		return false; /* unreachable */
	}

	switch (keytype) {
	case INT2OID:
		if (v < PG_INT16_MIN || v > PG_INT16_MAX)
			return false;
		*normalized = Int16GetDatum((int16)v);
		return true;
	case INT4OID:
		if (v < PG_INT32_MIN || v > PG_INT32_MAX)
			return false;
		*normalized = Int32GetDatum((int32)v);
		return true;
	case INT8OID:
		*normalized = Int64GetDatum(v);
		return true;
	default:
		/* validator pinned keytype; cannot happen */
		elog(ERROR, "unexpected reverse-key index key type %u", keytype);
		return false; /* unreachable */
	}
}

void
cluster_reverse_key_transform_scankeys(Relation indexRel, ScanKey keys, int nkeys)
{
	Form_pg_attribute att = TupleDescAttr(RelationGetDescr(indexRel), 0);
	Oid keytype = att->atttypid;
	int16 typlen = att->attlen;
	int i;

	for (i = 0; i < nkeys; i++) {
		ScanKey sk = &keys[i];

		if (sk->sk_attno != 1)
			continue; /* cannot happen on a single-column index */

		/*
		 * A NULL probe carries no datum and never equality-matches; the
		 * standard scan machinery already returns zero rows for it
		 * (_bt_fix_scankey_strategy fails the qual), so leave it alone.
		 */
		if (sk->sk_flags & SK_ISNULL)
			continue;

		/*
		 * The planner matches only plain equality OpExprs to a reverse-key
		 * index (Q18-A); a row-comparison header, an IN-list array probe,
		 * or a non-equality strategy here means that contract was violated.
		 * Searching with an un-reversed or range probe would silently
		 * return wrong rows, so fail closed instead.
		 */
		if (sk->sk_flags & (SK_ROW_HEADER | SK_SEARCHARRAY))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cluster_reverse_key index supports only plain equality lookups"),
					 errhint("Byte-reversal is not order-preserving; use a plain "
							 "index for row or array comparisons.")));
		if (sk->sk_strategy != BTEqualStrategyNumber)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cluster_reverse_key index supports only equality lookups"),
							errhint("Byte-reversal is not order-preserving; use a plain "
									"index for range or ordered scans.")));

		if (OidIsValid(sk->sk_subtype) && sk->sk_subtype != keytype) {
			Datum normalized;
			Oid eqop;
			Oid eqfunc;

			/*
			 * Cross-type probe (e.g. an int4 literal against an int8 key
			 * via integer_ops int84eq).  Byte-reversal is a bijection only
			 * at equal width, so normalize the probe to the key type and
			 * retarget the key at the same-type equality operator.  A
			 * narrowing probe outside the key type's range matches nothing:
			 * mark the key SK_ISNULL, which the scan machinery treats as an
			 * unsatisfiable qual (same path as a NULL runtime parameter) --
			 * zero rows, exactly the right answer.
			 */
			if (!reverse_key_normalize_probe(sk->sk_subtype, sk->sk_argument, keytype,
											 &normalized)) {
				sk->sk_flags |= SK_ISNULL;
				continue;
			}

			eqop = get_opfamily_member(indexRel->rd_opfamily[0], keytype, keytype,
									   BTEqualStrategyNumber);
			if (!OidIsValid(eqop))
				elog(ERROR, "missing same-type equality operator in opfamily %u",
					 indexRel->rd_opfamily[0]);
			eqfunc = get_opcode(eqop);
			if (!OidIsValid(eqfunc))
				elog(ERROR, "missing function for operator %u", eqop);

			fmgr_info(eqfunc, &sk->sk_func);
			sk->sk_subtype = keytype;
			sk->sk_argument = cluster_reverse_key_encode(normalized, typlen);
		} else
			sk->sk_argument = cluster_reverse_key_encode(sk->sk_argument, typlen);
	}
}
