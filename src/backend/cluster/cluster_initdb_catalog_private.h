/* Original catalog inputs and exclusive publication; no startup repair.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_INITDB_CATALOG_PRIVATE_H
#define CLUSTER_INITDB_CATALOG_PRIVATE_H

#include "cluster/cluster_catalog_init.h"
#include "cluster/cluster_recovery_file_identity.h"

/* Borrow a held, qualified original CLOG directory. Read complete pages for
 * the original native range, with bounded allocation and file rechecks.
 * Success transfers malloc'd bytes; failure leaves NULL/zero. The caller
 * independently binds the directory and retains/rechecks its original tree. */
extern bool cluster_initdb_catalog_read_clog(int directory, uint64 native_hw, uint8 **bytes,
											 Size *length);

/* The original creator supplies its independently selected initial CLOG0.
 * Only the complete BOOT first page is supported; no identity or missing
 * inventory is inferred from native_hw or the file. Failure is NULL/zero. */
extern bool cluster_initdb_catalog_read_managed_clog_v2(
	int directory, const ClusterRecoveryFileIdentityV2 *expected, uint64 native_hw,
	uint8 **bytes, Size *length);

/* The original creator alone publishes into its new shared global directory.
 * All inputs and destination absence are checked before the first write.
 * Partial failures remain unselected: no overwrite, adoption or cleanup.
 * Caller must recheck original sources and include outputs before ROOT-last. */
extern bool cluster_initdb_catalog_create(int global, const ClusterCatalogInitialInput *input);

#endif
