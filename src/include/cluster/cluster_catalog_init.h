/* Pure original catalog/allocator image construction.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_CATALOG_INIT_H
#define CLUSTER_CATALOG_INIT_H

#include "c.h"
#include "cluster/cluster_catalog_manifest.h"

typedef enum ClusterCatalogInitialKind {
	CLUSTER_CATALOG_INITIAL_OID = 0,
	CLUSTER_CATALOG_INITIAL_MARKER,
	CLUSTER_CATALOG_INITIAL_XID,
	CLUSTER_CATALOG_INITIAL_PREHISTORY
} ClusterCatalogInitialKind;

typedef struct ClusterCatalogInitialInput {
	ClusterCatalogManifestIdentity identity;
	const void *native_control;
	size_t native_control_length;
	const uint8 *native_clog;
	size_t native_clog_length;
} ClusterCatalogInitialInput;

/* A successful original creator supplies the exact shutdown control and
 * whole pg_xact pages covering [0, nextXid), in the verified namespace.
 * Return zero for invalid input/kind. No I/O or runtime activation. */
extern size_t cluster_catalog_initial_image_size(ClusterCatalogInitialKind kind,
												 const ClusterCatalogInitialInput *input);

/* Construct the original format into an exactly sized, all-zero output.
 * Any refusal leaves all bytes unchanged. Aliasing input/output is refused.
 * The caller owns O_EXCL, durability/readback and publication after DATA. */
extern bool cluster_catalog_initial_image(ClusterCatalogInitialKind kind,
										  const ClusterCatalogInitialInput *input, void *output,
										  size_t output_length);

#endif
