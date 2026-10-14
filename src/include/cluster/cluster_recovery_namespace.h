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
#include <sys/stat.h>
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
	CLUSTER_RECOVERY_FILE_RELMAP_AUTHORITY_BACKUP,
	CLUSTER_RECOVERY_FILE_UNDO_SEGMENT,
	/* A PostgreSQL two-phase state file is a durable recovery obligation.
	 * Its xid is only a physical key; this kind does not prove PREPARED or
	 * authorize COMMIT/ROLLBACK PREPARED. */
	CLUSTER_RECOVERY_FILE_TWOPHASE,
	/* A native SLRU segment is an explicit D1 physical obligation.  Its
	 * bytes still need the native page census before any recovery consumer may
	 * use them. */
	CLUSTER_RECOVERY_FILE_NATIVE_SLRU
} ClusterRecoveryFileKind;

/* One-based family values intentionally match the native-side observer's
 * family order without exposing that bootstrap-private header here. */
typedef enum ClusterRecoveryNativeFamily {
	CLUSTER_RECOVERY_NATIVE_FAMILY_NONE = 0,
	CLUSTER_RECOVERY_NATIVE_FAMILY_XACT = 1,
	CLUSTER_RECOVERY_NATIVE_FAMILY_SUBTRANS = 2,
	CLUSTER_RECOVERY_NATIVE_FAMILY_MULTIXACT = 3,
	CLUSTER_RECOVERY_NATIVE_FAMILY_COMMIT_TS = 4,
	CLUSTER_RECOVERY_NATIVE_FAMILY_MX_OFFSETS = 5,
	CLUSTER_RECOVERY_NATIVE_FAMILY_MX_MEMBERS = 6
} ClusterRecoveryNativeFamily;

typedef struct ClusterRecoveryNamespaceEntry {
	char path[MAXPGPATH];
	ClusterRecoveryFileKind kind;
	RelFileLocator locator;
	ForkNumber forknum;
	uint64 size;
	uint32 undo_segment;
	uint8 undo_owner_instance;
	uint32 twophase_xid;   /* canonical pg_twophase/<8 uppercase hex> key */
	uint8 native_family;   /* ClusterRecoveryNativeFamily, for native SLRU files */
	uint32 native_segment; /* canonical four-digit native segment number */
	/* Local observation only, never serialized or used as storage authority. */
	struct stat observed;
} ClusterRecoveryNamespaceEntry;

typedef struct ClusterRecoveryNamespace ClusterRecoveryNamespace;

/* A canonical name identifies a candidate, not its SPACE identity or contents.
 * Unknown names are returned explicitly; local native SLRU segments use their
 * own explicit kind and still require the page-level native census. */
extern ClusterRecoveryFileKind
cluster_recovery_namespace_classify(const char *path, RelFileLocator *locator, ForkNumber *forknum);

/* Extended classifier used by the D1 census.  The legacy three-argument
 * classifier remains source-compatible; native outputs are zeroed unless a
 * canonical local SLRU segment path is recognized. */
extern ClusterRecoveryFileKind cluster_recovery_namespace_classify_ex(const char *path,
																	  RelFileLocator *locator,
																	  ForkNumber *forknum,
																	  uint8 *native_family,
																	  uint32 *native_segment);

/* root_fd is borrowed from the original ROOT-selected, isolated namespace
 * owner. This scanner neither supplies nor renews that authority. max_entries
 * is a hard capacity: exhaustion is an error, never a complete empty suffix.
 * The caller destroys the scanner on every exit, including PG_FINALLY. */
extern ClusterRecoveryNamespaceResult
cluster_recovery_namespace_begin(int root_fd, uint32 max_entries, ClusterRecoveryNamespace **out);

/* Includes the directory stack and libc stream buffers. Stream reservations
 * are returned on close; an unsupported/unbounded libc/filesystem shape or
 * exhausted budget is CAPACITY, never END. No recovery authority is added. */
extern ClusterRecoveryNamespaceResult
cluster_recovery_namespace_begin_bounded(int root_fd, uint32 max_entries, size_t memory_budget,
										 ClusterRecoveryNamespace **out);

/* At most 128 directory items per call. ENTRY returns a borrowed file fd for
 * regular files, valid until next/destroy; directories return -1. Do not close
 * or mutate it. The next call rechecks the held object and its directory name.
 * Errors are sticky. END only completes this physical enumeration: the cold
 * owner must still check its ROOT/cuts, decode every producer and re-enumerate
 * after planned recovery changes. END cannot grant apply, OPEN or retirement. */
extern ClusterRecoveryNamespaceResult
cluster_recovery_namespace_next(ClusterRecoveryNamespace *scan, ClusterRecoveryNamespaceEntry *out,
								int *file_fd);

/* Read one bounded chunk of the currently held file, without advancing its
 * descriptor position. ENTRY means exactly length bytes, with the descriptor
 * and original directory name unchanged before and after the read. Length is
 * 1..BLCKSZ and the range must fit the enumerated size. Other results poison
 * the scan; a short read is IO, never EOF or an empty page. For a valid output
 * buffer/length, failure clears the output. This does not validate page bytes,
 * SPACE identity, authority, WAL coverage or recovery completeness. */
extern ClusterRecoveryNamespaceResult
cluster_recovery_namespace_read_current(ClusterRecoveryNamespace *scan, uint64 offset, void *buffer,
										size_t length);
extern void cluster_recovery_namespace_destroy(ClusterRecoveryNamespace *scan);
extern bool cluster_recovery_namespace_stat_same(const struct stat *before,
												 const struct stat *after);

#endif
