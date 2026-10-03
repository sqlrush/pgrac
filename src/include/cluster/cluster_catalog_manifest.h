/* Original catalog publication-set decoder, shared by frontend and backend.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_CATALOG_MANIFEST_H
#define CLUSTER_CATALOG_MANIFEST_H

#include "c.h"

#define CLUSTER_CATALOG_MANIFEST_MAX_BYTES 512

/* Memory carriers only; neither structure is a persistent representation. */
typedef struct ClusterCatalogManifestIdentity {
	uint8 authority_uuid[16];
	uint8 storage_uuid[16];
	uint64 database_incarnation;
	uint64 system_identifier;
} ClusterCatalogManifestIdentity;

typedef struct ClusterCatalogManifestInitial {
	ClusterCatalogManifestIdentity identity;
	uint64 generation;
	uint32 version;
	uint32 entry_count;
} ClusterCatalogManifestInitial;

/* Only the original generation-1, explicitly empty publication set is
 * supported. Require the complete canonical bytes, including one final LF,
 * and the independently selected namespace/generation/SHA256. This does not
 * prove that catalog DATA is empty, or grant serving or write authority.
 * No I/O, mutation, fallback or input normalization. Failure clears a distinct
 * out. Overlap with any input is refused without modifying either range. */
extern bool
cluster_catalog_manifest_decode_initial(const uint8 *bytes, size_t length,
										const ClusterCatalogManifestIdentity *expected_identity,
										uint64 expected_generation, const uint8 expected_sha256[32],
										ClusterCatalogManifestInitial *out);

#endif
