/*-------------------------------------------------------------------------
 * PGRAC: root-selected immutable configuration objects.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SHARED_CONFIG_H
#define CLUSTER_SHARED_CONFIG_H

#include "cluster/cluster_control_root.h"

#define CLUSTER_SHARED_CONFIG_MAX_BYTES (1024 * 1024)
#define CLUSTER_SHARED_CONFIG_MAX_ENTRIES 8192
#define CLUSTER_SHARED_CONFIG_MAX_NAME 127
#define CLUSTER_SHARED_CONFIG_MAX_VALUE 8192
#define CLUSTER_SHARED_CONFIG_COMMON (-1)

/* Logical in-memory carriers, not disk structs or startup permissions. */
typedef struct ClusterSharedConfigIdentity {
	uint64 system_identifier;
	uint64 database_incarnation;
	uint64 generation;
	uint8 storage_uuid[16];
	uint8 authority_uuid[16];
	uint64 configured[2];
} ClusterSharedConfigIdentity;

typedef struct ClusterSharedConfigRef {
	ClusterSharedConfigIdentity identity;
	uint8 sha256[32];
} ClusterSharedConfigRef;

typedef struct ClusterSharedConfigEntry {
	int node_id; /* -1 common, otherwise an explicitly configured node */
	const char *name;
	const char *value;
} ClusterSharedConfigEntry;

typedef struct ClusterSharedConfigImage {
	char *bytes; /* palloc-owned exact bytes plus a convenience NUL, not hashed */
	size_t len;
} ClusterSharedConfigImage;

StaticAssertDecl(sizeof(ClusterSharedConfigIdentity) == 72, "shared config identity carrier");
StaticAssertDecl(sizeof(ClusterSharedConfigRef) == 104, "shared config reference carrier");

/* Entries must already be in canonical key order; no implicit sorting or
 * duplicate resolution. Inputs/outputs must not alias. Refusal clears outputs.
 * These APIs validate representation and binding ONLY. Actual GUC name/context,
 * value, sensitive-reference and application policy is a separate mandatory
 * consumer gate; neither a decoded object nor lookup grants serving permission.
 */
extern ClusterControlRootResult
cluster_shared_config_encode(const ClusterSharedConfigIdentity *id,
							 const ClusterSharedConfigEntry *entries, size_t count, char *bytes,
							 size_t capacity, size_t *len, uint8 sha256[32]);
extern ClusterControlRootResult cluster_shared_config_validate(const char *bytes, size_t len,
															   const ClusterSharedConfigRef *ref,
															   uint32 *count);
extern ClusterControlRootResult cluster_shared_config_lookup(const char *bytes, size_t len,
															 const ClusterSharedConfigRef *ref,
															 int node_id, const char *name,
															 char *value, size_t capacity);

/* Borrow CF-S/X. Exact fixed path only; no directory search/projection/.bak.
 * Caller owns root selection/revalidation. No directories or objects created.
 * shared_root must be an absolute deployment path. The image is cleared on
 * failure; release successful output with cluster_shared_config_free().
 */
extern ClusterControlRootResult cluster_shared_config_read_locked(const char *shared_root,
																  const ClusterSharedConfigRef *ref,
																  ClusterSharedConfigImage *out);
extern void cluster_shared_config_free(ClusterSharedConfigImage *image);

#endif
