/*-------------------------------------------------------------------------
 *
 * cluster_shared_catalog.c
 *	  Shared system-catalog single-authority: pure decision helpers.
 *
 *	  spec-6.14 D1.  Side-effect-free predicates for the shared_catalog
 *	  feature: the startup dependency vet and the "non-temp = shared"
 *	  routing criterion.  Kept free of PG-internal dependencies so
 *	  cluster_unit links it standalone (test_cluster_shared_catalog).
 *	  The GUC registration, startup FATAL wiring and cross-node announce
 *	  consistency check live in cluster_guc.c / cluster_reconfig.c and call
 *	  into these helpers.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_shared_catalog.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-6.14-shared-catalog-single-authority.md (D1)
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"

#include "cluster/cluster_shared_catalog.h"

/*
 * cluster_shared_catalog_vet -- see header.  Priority order matters: report
 * the most foundational missing dependency first so the operator fixes one
 * thing at a time.
 */
ClusterSharedCatalogVetResult
cluster_shared_catalog_vet(bool shared_catalog,
						   bool smgr_user_relations,
						   bool have_shared_data_dir,
						   bool controlfile_shared_authority)
{
	if (!shared_catalog)
		return CLUSTER_SHARED_CATALOG_VET_OK;

	if (!smgr_user_relations)
		return CLUSTER_SHARED_CATALOG_VET_MISSING_SMGR_USER_RELATIONS;
	if (!have_shared_data_dir)
		return CLUSTER_SHARED_CATALOG_VET_MISSING_SHARED_DATA_DIR;
	if (!controlfile_shared_authority)
		return CLUSTER_SHARED_CATALOG_VET_MISSING_CF_AUTHORITY;

	return CLUSTER_SHARED_CATALOG_VET_OK;
}

/*
 * cluster_shared_catalog_vet_missing_dep_name -- see header.
 */
const char *
cluster_shared_catalog_vet_missing_dep_name(ClusterSharedCatalogVetResult r)
{
	switch (r)
	{
		case CLUSTER_SHARED_CATALOG_VET_OK:
			return "";
		case CLUSTER_SHARED_CATALOG_VET_MISSING_SMGR_USER_RELATIONS:
			return "cluster.smgr_user_relations=on";
		case CLUSTER_SHARED_CATALOG_VET_MISSING_SHARED_DATA_DIR:
			return "cluster.shared_data_dir";
		case CLUSTER_SHARED_CATALOG_VET_MISSING_CF_AUTHORITY:
			return "cluster.controlfile_shared_authority=on";
	}
	return "";
}
