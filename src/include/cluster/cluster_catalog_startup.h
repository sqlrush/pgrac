/* Original catalog inputs selected by the shared bootstrap owner.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_CATALOG_STARTUP_H
#define CLUSTER_CATALOG_STARTUP_H

#include "cluster/cluster_catalog_init.h"

typedef struct ClusterCatalogInputImage {
	const void *bytes;
	size_t length;
} ClusterCatalogInputImage;

typedef struct ClusterCatalogStartupInput {
	ClusterCatalogInitialInput original;
	ClusterCatalogInputImage manifest;
	uint64 catalog_generation;
	uint8 catalog_sha256[32];
	ClusterCatalogInputImage oid;
	ClusterCatalogInputImage marker;
	ClusterCatalogInputImage xid;
	ClusterCatalogInputImage xid_backup;
	ClusterCatalogInputImage prehistory;
} ClusterCatalogStartupInput;

/* Pure verification, never normalizes or writes any input. The original
 * native inputs must be independent of the current authority images. */
extern bool cluster_catalog_startup_validate(const ClusterCatalogStartupInput *input);

/* Register only from the postmaster's verified shared bootstrap owner.
 * read pins a ROOT selection and exact objects; current rechecks it after
 * validation; release frees the per-read resources on success or refusal.
 * Callbacks return failure rather than throwing. They never seed/repair.
 * This is a process-local call interface, not another authority. */
typedef struct ClusterCatalogStartupSource {
	bool (*read)(ClusterCatalogStartupInput *out, void *arg);
	bool (*current)(void *arg);
	void (*release)(void *arg);
	void *arg;
} ClusterCatalogStartupSource;

extern bool cluster_catalog_startup_set_source(const ClusterCatalogStartupSource *source);
extern bool cluster_catalog_startup_shared_verify(void);

#endif
