/*-------------------------------------------------------------------------
 *
 * cluster_catalog_migrate.h
 *	  Seed/adopt the shared catalog relation tree (spec-6.14 D2).
 *
 *	  When cluster.shared_catalog is on, catalog relation files live in ONE
 *	  shared tree under cluster.shared_data_dir (the same tree the sharedfs
 *	  backend routes permanent relations to once the D3 gate is flipped:
 *	  <shared_data_dir>/global/<relfile> and .../base/<db>/<relfile>).  The
 *	  SEED node copies its freshly-initdb'd catalog relation files into that
 *	  tree once; every JOIN node adopts the existing tree instead of writing a
 *	  divergent copy.  A durable authority marker under global/ is the
 *	  seed-vs-join discriminator and the fail-closed identity gate.
 *
 *	  This mirrors the spec-5.6 cluster_cf_migrate_and_link runtime pattern
 *	  (shared pg_control) one level up: pg_control is a single shared file,
 *	  the catalog tree is a set of shared relation files, but both are
 *	  established postmaster-once at startup with seed-if-absent semantics and
 *	  a system-identifier gate.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_catalog_migrate.h
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D2)
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_CATALOG_MIGRATE_H
#define CLUSTER_CATALOG_MIGRATE_H

#include "c.h"
#include "port/pg_crc32c.h"

/*
 * Durable authority marker.  Written LAST (atomically, after every catalog
 * relation file is copied and fsync'd) so its presence proves the shared
 * catalog tree is complete and trustworthy.  A torn or partial seed leaves no
 * marker, so the next start re-seeds rather than adopting a half-populated
 * tree.  Fixed-size + CRC = torn-safe, exactly like the spec-5.6 CF authority.
 */
#define CLUSTER_CATALOG_AUTHORITY_MAGIC 0x50474341 /* 'PGCA' */
#define CLUSTER_CATALOG_AUTHORITY_VERSION 1
#define CLUSTER_CATALOG_AUTHORITY_REL_PATH "global/pgrac_catalog_authority"
#define CLUSTER_CATALOG_AUTHORITY_TMP_REL_PATH "global/pgrac_catalog_authority.tmp"

typedef struct ClusterCatalogAuthorityMarker {
	uint32 magic;
	uint32 version;
	uint64 system_identifier;  /* must match the shared pg_control */
	uint32 catalog_version_no; /* must match this build's CATALOG_VERSION_NO */
	uint32 flags;			   /* reserved (0) */
	pg_crc32c crc;			   /* over [0, offsetof(crc)) */
} ClusterCatalogAuthorityMarker;

/*
 * cluster_catalog_migrate_tree -- establish the shared catalog relation tree
 *	for this node.  Postmaster-once, only meaningful when
 *	cluster.shared_catalog is on.
 *
 *	SEED (marker absent): copy every catalog relation file from local_pgdata's
 *	global/ and base/<db>/ into the shared tree, then write the marker.
 *	JOIN (marker present): verify the marker's system identifier and catalog
 *	version match this node, then adopt the existing tree (no copy).
 *
 *	Fail-closed: FATAL 53RB0 on a foreign/mismatched or unreadable authority.
 *	system_identifier is the cluster-wide value from the shared pg_control.
 */
extern void cluster_catalog_migrate_tree(const char *local_pgdata, uint64 system_identifier);

/*
 * cluster_catalog_vet_no_unlogged -- spec-6.14 Q12 enable-time vet: FATAL
 *	(FEATURE_NOT_SUPPORTED) if any unlogged relation storage (an "_init" fork
 *	file) exists in the local tree or the shared tree.  Runs every
 *	shared-catalog boot, before any adopt/seed side effect.
 */
extern void cluster_catalog_vet_no_unlogged(const char *local_pgdata);

/*
 * cluster_catalog_vet_off_mode -- spec-6.14 D5 off-flip vet: FATAL when
 *	cluster.shared_catalog is off but the shared tree holds a catalog
 *	authority marker (the local catalog files are stale once any DDL ran
 *	under the shared catalog).  Postmaster-once, off-mode boots only.
 */
extern void cluster_catalog_vet_off_mode(void);

#endif /* CLUSTER_CATALOG_MIGRATE_H */
