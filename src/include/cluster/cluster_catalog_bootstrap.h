/*-------------------------------------------------------------------------
 *
 * cluster_catalog_bootstrap.h
 *	  Shared-catalog runtime bootstrap: seed the durable authorities and
 *	  (later) migrate/adopt the catalog tree (spec-6.14 D2).
 *
 *	  Mirrors the spec-5.6 cluster_cf_startup_prepare pattern: at postmaster
 *	  startup, when cluster.shared_catalog is on, the SEED node creates the
 *	  shared OID / relmap authorities (and the shared catalog tree), while a
 *	  JOIN node adopts the existing ones -- "seed if absent" naturally
 *	  distinguishes the two for the normal seed-then-join bring-up.  The seed
 *	  values come from the shared pg_control (the authority that
 *	  shared_catalog=on already requires, D1 vet), so seed and join agree.
 *
 *	  This increment seeds the OID authority; the relmap-authority seed and the
 *	  catalog-file migration/adoption land with D2b/D2c and D5-activation.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_catalog_bootstrap.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D2)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CATALOG_BOOTSTRAP_H
#define CLUSTER_CATALOG_BOOTSTRAP_H

/*
 * cluster_catalog_startup_prepare -- postmaster-once.  No-op unless
 *	cluster.shared_catalog is on.  Seeds the shared OID authority from the
 *	shared pg_control high-water (seed node), or adopts it (join node).
 *	Fail-closed FATAL (53RB0) when the shared authority the feature depends on
 *	is unavailable.
 */
extern void cluster_catalog_startup_prepare(void);

#endif							/* CLUSTER_CATALOG_BOOTSTRAP_H */
