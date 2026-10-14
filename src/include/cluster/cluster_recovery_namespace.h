/*-------------------------------------------------------------------------
 * cluster_recovery_namespace.h
 *   Read-only physical candidates for cold recovery.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_NAMESPACE_H
#define CLUSTER_RECOVERY_NAMESPACE_H

#include "c.h"
#include "common/relpath.h"
#include "storage/relfilelocator.h"

typedef enum ClusterRecoveryNamespaceResult {
	CLUSTER_RECOVERY_NAMESPACE_ENTRY = 0,
	CLUSTER_RECOVERY_NAMESPACE_MORE,
	CLUSTER_RECOVERY_NAMESPACE_END,
	CLUSTER_RECOVERY_NAMESPACE_INVALID,
	CLUSTER_RECOVERY_NAMESPACE_IO,
	CLUSTER_RECOVERY_NAMESPACE_CHANGED,
	CLUSTER_RECOVERY_NAMESPACE_UNSAFE,
	CLUSTER_RECOVERY_NAMESPACE_CAPACITY
} ClusterRecoveryNamespaceResult;

typedef enum ClusterRecoveryFileKind {
	CLUSTER_RECOVERY_FILE_UNCLASSIFIED = 0,
	CLUSTER_RECOVERY_FILE_DIRECTORY,
	CLUSTER_RECOVERY_FILE_RELATION,
	CLUSTER_RECOVERY_FILE_RELMAP,
	CLUSTER_RECOVERY_FILE_RELMAP_BACKUP,
	CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY,
	CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY_BACKUP
} ClusterRecoveryFileKind;

typedef struct ClusterRecoveryNamespaceEntry {
	char path[MAXPGPATH];
	ClusterRecoveryFileKind kind;
	RelFileLocator locator;
	ForkNumber forknum;
	uint64 size;
} ClusterRecoveryNamespaceEntry;

typedef struct ClusterRecoveryNamespace ClusterRecoveryNamespace;

/* A canonical name identifies a candidate, not its SPACE identity or contents.
 * Unknown names are returned explicitly, including native segmented files:
 * cluster_fs stores one flat file per fork. No unknown name means no work. */
extern ClusterRecoveryFileKind
cluster_recovery_namespace_classify(const char *path, RelFileLocator *locator, ForkNumber *forknum);

/* root_fd is borrowed from the original ROOT-selected, isolated namespace
 * owner. This scanner neither supplies nor renews that authority. max_entries
 * is a hard capacity: exhaustion is an error, never a complete empty suffix.
 * The caller destroys the scanner on every exit, including PG_FINALLY. */
extern ClusterRecoveryNamespaceResult
cluster_recovery_namespace_begin(int root_fd, uint32 max_entries, ClusterRecoveryNamespace **out);

/* At most 128 directory items per call. ENTRY returns a borrowed file fd for
 * regular files, valid until next/destroy; directories return -1. Do not close
 * or mutate it. The next call rechecks the held object and its directory name.
 * Errors are sticky. END only completes this physical enumeration: the cold
 * owner must still check its ROOT/cuts, decode every producer and re-enumerate
 * after planned recovery changes. END cannot grant apply, OPEN or retirement. */
extern ClusterRecoveryNamespaceResult
cluster_recovery_namespace_next(ClusterRecoveryNamespace *scan, ClusterRecoveryNamespaceEntry *out,
								int *file_fd);
extern void cluster_recovery_namespace_destroy(ClusterRecoveryNamespace *scan);

#endif
