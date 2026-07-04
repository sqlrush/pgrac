/*-------------------------------------------------------------------------
 *
 * cluster_catalog_bootstrap.c
 *	  Shared-catalog runtime bootstrap (spec-6.14 D2).
 *
 *	  cluster_catalog_startup_prepare() runs postmaster-once at startup.  When
 *	  cluster.shared_catalog is on it seeds the shared OID authority from the
 *	  shared pg_control's next-OID high-water (a value both seed and join nodes
 *	  read identically), so the OID lease (D6) has a durable authority to draw
 *	  from.  "Seed if absent" makes this idempotent: the seed node (authority
 *	  absent) creates it, a join node (authority present) skips it.
 *
 *	  The relmap-authority seed and the catalog-file migration/adoption (the
 *	  rest of D2) land with D2b/D2c and D5-activation.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_catalog_bootstrap.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D2)
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "catalog/pg_control.h"
#include "cluster/cluster_catalog_bootstrap.h"
#include "cluster/cluster_cf_authority.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_oid_lease.h"
#include "miscadmin.h"

void
cluster_catalog_startup_prepare(void)
{
	ControlFileData cf;

	if (!cluster_shared_catalog)
		return;					/* off: stock per-node catalog */

	/* Postmaster-once: only the postmaster seeds; forked backends inherit. */
	if (IsUnderPostmaster)
		return;

	/*
	 * shared_catalog=on requires the shared pg_control authority (D1 vet), so
	 * it is the single source of the cluster-wide next-OID high-water.  Reading
	 * it fail-closed keeps a seed value that every node agrees on.
	 */
	if (!cluster_cf_authority_read(&cf))
		ereport(FATAL,
				(errcode(ERRCODE_CLUSTER_CATALOG_AUTHORITY_UNAVAILABLE),
				 errmsg("shared pg_control authority is unavailable at catalog bootstrap"),
				 errhint("cluster.shared_catalog requires a readable shared "
						 "pg_control under cluster.shared_data_dir.")));

	/*
	 * Seed the OID authority from the shared high-water if absent (seed node);
	 * a join node whose authority already exists is a no-op.  The high-water is
	 * never lowered, so a fresh cluster starts allocating cluster-wide OIDs
	 * from the same floor every node observes.
	 */
	if (cluster_oid_authority_seed_if_absent(cf.checkPointCopy.nextOid))
		elog(LOG, "cluster shared_catalog: seeded OID authority high-water at %u",
			 cf.checkPointCopy.nextOid);
}
