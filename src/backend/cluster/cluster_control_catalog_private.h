/* Selected catalog startup bytes; no creation or serving permission.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_CONTROL_CATALOG_PRIVATE_H
#define CLUSTER_CONTROL_CATALOG_PRIVATE_H

#include "cluster/cluster_catalog_startup.h"
#include "cluster_control_bootstrap_private.h"

typedef struct ClusterControlCatalogRead ClusterControlCatalogRead;

/* Caller owns a hash resource owner and always releases *read, including on
 * error. No descriptor survives a backend allocation or return. The expected
 * observation is the same process's early preparation, never a new selection.
 * Success borrows buffers until release. Refusal clears a distinct out. */
extern bool cluster_control_catalog_read(const char *pgdata, const char *shared,
	const ClusterControlBootstrapSnapshot *expected, ClusterControlCatalogRead **read,
	ClusterCatalogStartupInput *out);
extern bool cluster_control_catalog_current(ClusterControlCatalogRead *read);
extern void cluster_control_catalog_release(ClusterControlCatalogRead **read);

#endif
