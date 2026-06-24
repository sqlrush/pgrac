/*-------------------------------------------------------------------------
 *
 * cluster_ts_srf.c
 *	  pgrac TEST-ONLY SQL entry points driving the TT (tablespace-DDL) GES
 *	  acquire path for the spec-5.7 D5 TAP (t/296).
 *
 *	  cluster_ts_acquire_probe(mode text, kind text, key text, do_hold bool)
 *	      -> text
 *	    Drives the real TT(X)/TT(S) acquire (cluster_ts_test_acquire) on a resid
 *	    built from `kind` ('oid' -> resid = key::oid; 'name' -> resid =
 *	    hash(key)) at `mode` ('x' = TT(X), 's' = TT(S)).  Returns 'granted' /
 *	    'native', or -- faithfully mirroring the tablespace.c DDL hooks -- raises
 *	    53RA8 (ERRCODE_CLUSTER_TABLESPACE_LOCK_CONFLICT) on a conflict.
 *	    do_hold=true keeps the GES holder registered in this backend (so a
 *	    competing claim from a second session/node sees the conflict);
 *	    cluster_ts_release_probe() drops it.
 *
 *	  cluster_ts_release_probe() -> bool
 *	    Releases a held probe claim (returns whether one was held).
 *
 *	  This drives the REAL GES TT mutex with REAL competing claims (the holder
 *	  here, a real DDL on the peer in t/296).  TEST-ONLY, superuser-only.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.7-misc-enqueue-classes.md (D5, §3.3)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ts_srf.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

PG_FUNCTION_INFO_V1(cluster_ts_acquire_probe);
PG_FUNCTION_INFO_V1(cluster_ts_release_probe);

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_ts.h"
#include "miscadmin.h" /* superuser */
#include "storage/lock.h"
#include "utils/builtins.h"

/* One held probe claim per backend (survives xact end -- the test-only acquire
 * does not register the production top-xact-end callback), so a competing session
 * can observe the conflict.  One claim per backend -> a file static is exact. */
static ClusterTsLock probe_lock;
static bool probe_held = false;

Datum
cluster_ts_acquire_probe(PG_FUNCTION_ARGS)
{
	char *mode_s;
	char *kind_s;
	char *key_s;
	bool do_hold;
	LOCKMODE mode;
	ClusterResId resid;
	ClusterTsLock lk;
	int rc;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_ts_acquire_probe is superuser-only")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2) || PG_ARGISNULL(3))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("mode, kind, key and do_hold must not be NULL")));

	mode_s = text_to_cstring(PG_GETARG_TEXT_PP(0));
	kind_s = text_to_cstring(PG_GETARG_TEXT_PP(1));
	key_s = text_to_cstring(PG_GETARG_TEXT_PP(2));
	do_hold = PG_GETARG_BOOL(3);

	if (strcmp(mode_s, "x") == 0)
		mode = ExclusiveLock;
	else if (strcmp(mode_s, "s") == 0)
		mode = ShareLock;
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("mode must be 'x' or 's'")));

	if (strcmp(kind_s, "oid") == 0)
		cluster_ts_resid_encode_oid((Oid)strtoul(key_s, NULL, 10), &resid);
	else if (strcmp(kind_s, "name") == 0)
		cluster_ts_resid_encode_name(key_s, &resid);
	else
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("kind must be 'oid' or 'name'")));

	rc = cluster_ts_test_acquire(&resid, mode, &lk);

	if (rc == 0) {
		/* granted */
		if (do_hold) {
			if (probe_held)
				cluster_ts_test_release(&probe_lock);
			probe_lock = lk;
			probe_held = true;
		} else {
			cluster_ts_test_release(&lk);
		}
		PG_RETURN_TEXT_P(cstring_to_text("granted"));
	}
	if (rc == 1)
		PG_RETURN_TEXT_P(cstring_to_text("native"));

	/* rc == 2: conflict -- faithful to the tablespace.c hooks' fail-closed. */
	ereport(ERROR, (errcode(ERRCODE_CLUSTER_TABLESPACE_LOCK_CONFLICT),
					errmsg("could not acquire the cluster tablespace %s lock (kind=%s key=%s)",
						   mode == ShareLock ? "TT(S)" : "TT(X)", kind_s, key_s)));
	PG_RETURN_NULL(); /* unreachable */
}

Datum
cluster_ts_release_probe(PG_FUNCTION_ARGS)
{
	bool was_held;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_ts_release_probe is superuser-only")));

	was_held = probe_held;
	if (probe_held) {
		cluster_ts_test_release(&probe_lock);
		probe_held = false;
	}
	PG_RETURN_BOOL(was_held);
}

#else /* !USE_PGRAC_CLUSTER */

Datum
cluster_ts_acquire_probe(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_ts_acquire_probe requires --enable-cluster")));
	PG_RETURN_NULL();
}

Datum
cluster_ts_release_probe(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_ts_release_probe requires --enable-cluster")));
	PG_RETURN_NULL();
}

#endif /* USE_PGRAC_CLUSTER */
