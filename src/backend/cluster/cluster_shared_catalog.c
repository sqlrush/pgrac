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

#include <stdlib.h> /* strtol */

#include "cluster/cluster_shared_catalog.h"

/*
 * cluster_shared_catalog_vet -- see header.  Priority order matters: report
 * the most foundational missing dependency first so the operator fixes one
 * thing at a time.
 */
ClusterSharedCatalogVetResult
cluster_shared_catalog_vet(bool shared_catalog, bool smgr_user_relations, bool have_shared_data_dir,
						   bool controlfile_shared_authority, bool merged_recovery)
{
	if (!shared_catalog)
		return CLUSTER_SHARED_CATALOG_VET_OK;

	if (!smgr_user_relations)
		return CLUSTER_SHARED_CATALOG_VET_MISSING_SMGR_USER_RELATIONS;
	if (!have_shared_data_dir)
		return CLUSTER_SHARED_CATALOG_VET_MISSING_SHARED_DATA_DIR;
	if (!controlfile_shared_authority)
		return CLUSTER_SHARED_CATALOG_VET_MISSING_CF_AUTHORITY;
	if (!merged_recovery)
		return CLUSTER_SHARED_CATALOG_VET_MISSING_MERGED_RECOVERY;

	return CLUSTER_SHARED_CATALOG_VET_OK;
}

/*
 * cluster_shared_catalog_vet_missing_dep_name -- see header.
 */
const char *
cluster_shared_catalog_vet_missing_dep_name(ClusterSharedCatalogVetResult r)
{
	switch (r) {
	case CLUSTER_SHARED_CATALOG_VET_OK:
		return "";
	case CLUSTER_SHARED_CATALOG_VET_MISSING_SMGR_USER_RELATIONS:
		return "cluster.smgr_user_relations=on";
	case CLUSTER_SHARED_CATALOG_VET_MISSING_SHARED_DATA_DIR:
		return "cluster.shared_data_dir";
	case CLUSTER_SHARED_CATALOG_VET_MISSING_CF_AUTHORITY:
		return "cluster.controlfile_shared_authority=on";
	case CLUSTER_SHARED_CATALOG_VET_MISSING_MERGED_RECOVERY:
		return "cluster.merged_recovery=on";
	}
	return "";
}

/*
 * cluster_temp_namespace_format_suffix -- see header.  on-mode:
 * "n<node>_<backendId>"; off-mode: "<backendId>".
 */
void
cluster_temp_namespace_format_suffix(char *buf, size_t buflen, bool shared_catalog, int node,
									 int backend_id)
{
	if (shared_catalog)
		snprintf(buf, buflen, "n%d_%d", node, backend_id);
	else
		snprintf(buf, buflen, "%d", backend_id);
}

/*
 * cluster_temp_namespace_parse_suffix -- see header.  A leading 'n' marks the
 * node-qualified format "n<node>_<backendId>"; otherwise the stock "<backendId>".
 */
int
cluster_temp_namespace_parse_suffix(const char *suffix, int *out_node)
{
	long backend_id;
	char *endp;

	if (out_node != NULL)
		*out_node = -1;

	if (suffix == NULL)
		return -1;

	if (suffix[0] == 'n') {
		long node;

		/* node-qualified: n<node>_<backendId> */
		node = strtol(suffix + 1, &endp, 10);
		if (endp == suffix + 1 || *endp != '_')
			return -1; /* malformed */
		backend_id = strtol(endp + 1, &endp, 10);
		if (*(endp) != '\0' && *endp != '_') /* tolerate nothing after */
			/* trailing garbage: still accept the parsed id */;
		if (out_node != NULL)
			*out_node = (int)node;
		return (int)backend_id;
	}

	/* stock: <backendId> */
	backend_id = strtol(suffix, &endp, 10);
	if (endp == suffix)
		return -1;
	return (int)backend_id;
}
