/*-------------------------------------------------------------------------
 *
 * cluster_ir_srf.c
 *	  pgrac TEST-ONLY SQL entry points driving the IR (instance-recovery owner)
 *	  GES acquire path for the spec-5.7 D8 mechanism-level TAP (t/295).
 *
 *	  cluster_ir_acquire_probe(dead_node int4, epoch_offset int8, do_hold bool)
 *	      -> text
 *	    Drives cluster_ir_recovery_acquire() exactly as the recovery worker does:
 *	    the real GES IR(X) acquire over (dead_node, accepted_epoch + epoch_offset).
 *	    Returns 'owner' / 'native' / 'not_ready', or -- faithfully mirroring the
 *	    recovery worker's mutation gate -- raises 53RA9 (ERRCODE_CLUSTER_RECOVERY_
 *	    OWNER_CONFLICT) when a peer already holds IR(X) (NOT_OWNER).  do_hold=true
 *	    keeps the GES holder registered in this backend (so a competing claim from
 *	    a second session/node sees the conflict); cluster_ir_release_probe() drops
 *	    it.  A held claim is also reclaimed when the backend exits.
 *
 *	  cluster_ir_release_probe() -> bool
 *	    Releases a held probe claim (returns whether one was held).
 *
 *	  This is the mechanism-level proof that the IR(X) lock provides GES-enforced
 *	  recovery-owner uniqueness: exactly one claimant gets 'owner', a competing
 *	  claimant fails closed with 53RA9, and a stale launch epoch is refused
 *	  ('not_ready').  It drives the REAL acquire path with REAL competing claims;
 *	  it does NOT reproduce a true reconfig with divergent survivor alive-set views
 *	  (online thread recovery is 2-node-scoped, where the sole survivor self-masters
 *	  the IR resid -- see t/295's recorded e2e gap).
 *
 *	  TEST-ONLY: diagnostic entry points, NOT product query interfaces;
 *	  superuser-only.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-5.7-misc-enqueue-classes.md (D8, §3.4)
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_ir_srf.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"

PG_FUNCTION_INFO_V1(cluster_ir_acquire_probe);
PG_FUNCTION_INFO_V1(cluster_ir_release_probe);

#ifdef USE_PGRAC_CLUSTER

#include "cluster/cluster_epoch.h" /* cluster_epoch_get_current */
#include "cluster/cluster_ir.h"
#include "miscadmin.h" /* superuser */
#include "utils/builtins.h"

/*
 * The probe holds at most one IR(X) claim per backend across SQL statements (the
 * GES holder survives transaction end -- IR has no PG-native lock for the
 * LockReleaseAll hook to cover, mirroring DL), so a competing session can observe
 * the conflict.  One claim per backend, so a file static is exact.
 */
static ClusterIrLock probe_lock;
static bool probe_held = false;

Datum
cluster_ir_acquire_probe(PG_FUNCTION_ARGS)
{
	int32 dead_node;
	int64 epoch_offset;
	bool do_hold;
	uint64 episode_epoch;
	ClusterIrLock lk;
	ClusterIrAcquireOutcome out;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_ir_acquire_probe is superuser-only")));

	if (PG_ARGISNULL(0) || PG_ARGISNULL(1) || PG_ARGISNULL(2))
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("dead_node, epoch_offset and do_hold must not be NULL")));

	dead_node = PG_GETARG_INT32(0);
	epoch_offset = PG_GETARG_INT64(1);
	do_hold = PG_GETARG_BOOL(2);

	/* A negative epoch_offset wraps to a large uint64 that cannot equal the
	 * accepted epoch -> bootstrap_ready() is false -> 'not_ready' (fail-closed),
	 * which is exactly the desired stale-epoch verdict for the test. */
	episode_epoch = cluster_epoch_get_current() + (uint64)epoch_offset;
	out = cluster_ir_recovery_acquire(dead_node, episode_epoch, &lk);

	switch (out) {
	case CLUSTER_IR_OWNER:
		if (do_hold) {
			/* Keep the GES holder registered for a competing claim elsewhere;
			 * remember it so the release probe (or backend exit) can drop it. */
			if (probe_held)
				cluster_ir_recovery_release(&probe_lock);
			probe_lock = lk;
			probe_held = true;
		} else {
			cluster_ir_recovery_release(&lk);
		}
		PG_RETURN_TEXT_P(cstring_to_text("owner"));

	case CLUSTER_IR_NATIVE:
		PG_RETURN_TEXT_P(cstring_to_text("native"));

	case CLUSTER_IR_NOT_READY:
		PG_RETURN_TEXT_P(cstring_to_text("not_ready"));

	case CLUSTER_IR_NOT_OWNER:
		/* Faithful mutation-gate fail-closed: exactly what the recovery worker
		 * raises when a non-owner reaches the destructive apply (8.A). */
		ereport(
			ERROR,
			(errcode(ERRCODE_CLUSTER_RECOVERY_OWNER_CONFLICT),
			 errmsg("not the instance-recovery owner for dead node %d (episode " UINT64_FORMAT ")",
					dead_node, episode_epoch)));
	}

	PG_RETURN_NULL(); /* unreachable: switch is exhaustive */
}

Datum
cluster_ir_release_probe(PG_FUNCTION_ARGS)
{
	bool was_held;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("cluster_ir_release_probe is superuser-only")));

	was_held = probe_held;
	if (probe_held) {
		cluster_ir_recovery_release(&probe_lock);
		probe_held = false;
	}
	PG_RETURN_BOOL(was_held);
}

#else /* !USE_PGRAC_CLUSTER */

Datum
cluster_ir_acquire_probe(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_ir_acquire_probe requires --enable-cluster")));
	PG_RETURN_NULL();
}

Datum
cluster_ir_release_probe(PG_FUNCTION_ARGS pg_attribute_unused())
{
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("cluster_ir_release_probe requires --enable-cluster")));
	PG_RETURN_NULL();
}

#endif /* USE_PGRAC_CLUSTER */
