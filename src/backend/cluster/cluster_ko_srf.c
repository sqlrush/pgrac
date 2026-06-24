/*-------------------------------------------------------------------------
 *
 * cluster_ko_srf.c
 *	  pgrac TEST-ONLY SQL entry point driving the KO (object-reuse flush)
 *	  cross-node barrier for the spec-5.7 D6 TAP (t/297).
 *
 *	  cluster_ko_flush_probe(spc oid, db oid, rel oid) -> text
 *	    Drives the REAL barrier (cluster_ko_flush_and_wait_ack) on a synthetic
 *	    relfilenode {spc, db, rel} as a permanent relation: acquires KO(X),
 *	    fanouts PGRAC_IC_MSG_KO_FLUSH to every alive peer, and waits for each
 *	    peer's apply-after-drop ACK.  Returns 'ok' when the barrier is satisfied
 *	    (including the no-op cases), or -- faithfully mirroring the storage.c
 *	    DROP/TRUNCATE hooks -- raises 53RAA (ERRCODE_CLUSTER_OBJECT_FLUSH_-
 *	    UNAVAILABLE) when a peer does not ACK in time.
 *
 *	    This exercises the REAL cross-node protocol end to end: the real IC
 *	    fanout to the real peer, the real peer-side SI-Broadcaster-aux flush +
 *	    drop, and the real ACK round trip.  The relfilenode is synthetic (no
 *	    actual buffers on the peer, so the drop is a no-op), because a real
 *	    SHARED relation visible on both nodes needs cross-node catalog OID
 *	    coherence (Stage-3); the barrier MACHINERY is fully real.  TEST-ONLY,
 *	    superuser-only.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.7-misc-enqueue-classes.md (D6, §3.5/§3.6)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ko_srf.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

PG_FUNCTION_INFO_V1(cluster_ko_flush_probe);

#ifdef USE_PGRAC_CLUSTER

#include "catalog/pg_class.h" /* RELPERSISTENCE_PERMANENT */
#include "cluster/cluster_ko.h"
#include "miscadmin.h" /* superuser */
#include "storage/relfilelocator.h"
#include "utils/builtins.h"

Datum
cluster_ko_flush_probe(PG_FUNCTION_ARGS)
{
	RelFileLocator rloc;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_ko_flush_probe is superuser-only")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("spc, db and rel must not be NULL")));

	rloc.spcOid = PG_GETARG_OID(0);
	rloc.dbOid = PG_GETARG_OID(1);
	rloc.relNumber = (RelFileNumber)PG_GETARG_OID(2);

	/* Drives the real barrier; raises 53RAA on a peer that does not ACK. */
	cluster_ko_flush_and_wait_ack(rloc, RELPERSISTENCE_PERMANENT);

	PG_RETURN_TEXT_P(cstring_to_text("ok"));
}

#else /* !USE_PGRAC_CLUSTER */

Datum
cluster_ko_flush_probe(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_ko_flush_probe requires --enable-cluster")));
	PG_RETURN_NULL();
}

#endif /* USE_PGRAC_CLUSTER */
