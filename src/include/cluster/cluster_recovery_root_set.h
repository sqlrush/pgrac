/*-------------------------------------------------------------------------
 * cluster_recovery_root_set.h
 *   Union of physical namespace observations and retained WAL targets.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_ROOT_SET_H
#define CLUSTER_RECOVERY_ROOT_SET_H

#include "cluster/cluster_recovery_namespace.h"
#include "cluster/cluster_page_stable_base.h"
#include "cluster/cluster_space_identity.h"

#ifdef USE_PGRAC_CLUSTER

typedef enum ClusterRecoveryRootsResult {
	CLUSTER_RECOVERY_ROOTS_MORE = 0,
	CLUSTER_RECOVERY_ROOTS_ENTRY,
	CLUSTER_RECOVERY_ROOTS_BYTES,
	CLUSTER_RECOVERY_ROOTS_OBSERVED,
	CLUSTER_RECOVERY_ROOTS_INVALID,
	CLUSTER_RECOVERY_ROOTS_IO,
	CLUSTER_RECOVERY_ROOTS_CHANGED,
	CLUSTER_RECOVERY_ROOTS_UNSAFE,
	CLUSTER_RECOVERY_ROOTS_CAPACITY
} ClusterRecoveryRootsResult;

#define CLUSTER_RECOVERY_ROOT_FROM_WAL 1
#define CLUSTER_RECOVERY_ROOT_PHYSICAL 2

typedef struct ClusterRecoveryRootEntry {
	ClusterRecoveryNamespaceEntry file;
	uint32 origins;			  /* FROM_WAL and/or PHYSICAL; neither discharges an obligation */
	uint64 required_blocks;	  /* maximum referenced block + 1, NOT a file-size promise */
	uint8 content_sha256[32]; /* entire regular file; zero for a directory/missing file */
} ClusterRecoveryRootEntry;

typedef struct ClusterRecoveryRootsEvent {
	const ClusterRecoveryRootEntry *entry;
	const uint8 *bytes; /* current chunk, valid until next step/destroy */
	uint64 offset;
	size_t length;
	unsigned pass; /* 0 = discovery, 1 = full re-observation */
} ClusterRecoveryRootsEvent;

typedef struct ClusterRecoveryRootSet ClusterRecoveryRootSet;

/* Counts of canonical native SLRU segment files present in the stable
 * physical root union.  This is an observation only: page-level native
 * census, horizon checks, and the recovery owner must still prove every
 * family before any consumer may use it. */
extern bool cluster_recovery_root_set_native_counts(const ClusterRecoveryRootSet *roots,
													uint32 counts[6], uint32 *total);

/* The original cold owner supplies its retained, isolated ROOT-selected fd
 * and namespace. This component borrows the fd and establishes no authority.
 * Destroy on every exit, including PG_FINALLY. No target I/O or sync occurs. */
extern ClusterRecoveryRootsResult
cluster_recovery_root_set_begin(int root_fd, uint64 system_identifier, uint64 database_incarnation,
								const uint8 storage_uuid[16], size_t memory_budget,
								ClusterRecoveryRootSet **out);

/* Before the first step only. Preserve every typed PAGE target, including a
 * currently missing fork or blocks beyond EOF; the typed owner must explain
 * CREATE/DROP/TRUNCATE. Original record/source/incarnation proofs stay in the
 * cold plan, not in this physical-path index. Other producers are not implied. */
extern ClusterRecoveryRootsResult
cluster_recovery_root_set_add_target(ClusterRecoveryRootSet *roots, const RfPageIdentityV1 *page);

/* Whole-fork/structural candidates use the full namespace, without inventing
 * a page reference. required_blocks is an observed WAL requirement only.
 * These calls are allowed only before the first physical step. */
extern bool cluster_recovery_root_set_targets_match(const ClusterRecoveryRootSet *roots,
													uint64 system_identifier,
													uint64 database_incarnation,
													const uint8 storage_uuid[16]);
extern ClusterRecoveryRootsResult
cluster_recovery_root_set_add_fork(ClusterRecoveryRootSet *roots,
								   const ClusterSpaceIdentityKey *key, ForkNumber forknum,
								   uint64 required_blocks);
/* A failed producer must not leave its partial target set usable. */
extern void cluster_recovery_root_set_invalidate(ClusterRecoveryRootSet *roots);

struct ClusterRecoveryNativeEffect;
/* Add one native CREATE fork or a terminal's relation deletion (all shared
 * forks) to the same physical union. These target whole files, so they do
 * not invent a block-zero reference or a size promise. Only CREATE and
 * RELATION_DROP effects are accepted; all other effects need their own
 * consumers. Source/record and lifecycle proofs remain in the cold graph.
 * A missing file is retained, never treated as an already-completed drop. */
extern ClusterRecoveryRootsResult
cluster_recovery_root_set_add_native(ClusterRecoveryRootSet *roots,
									 const struct ClusterRecoveryNativeEffect *effect);

/* At most one BLCKSZ read or 128 metadata/sort operations per call; each
 * path lookup separately permits at most 128 bucket probes, then CAPACITY.
 * Hash exhaustion never removes a candidate or completes an observation. Observe
 * all files twice, not only WAL targets. Unknown files remain UNCLASSIFIED.
 * Directory/name/file identity and complete bytes must agree. OBSERVED is
 * only a stable candidate root set: missing/unknown/undecoded obligations
 * still prevent D1 completion. No terminal, apply, OPEN or GC permission. */
extern ClusterRecoveryRootsResult cluster_recovery_root_set_step(ClusterRecoveryRootSet *roots,
																 ClusterRecoveryRootsEvent *out);
/* After dependent graph traversal, re-observe the entire original set.
 * Follow with step(): at most one block / 128 metadata operations per call.
 * The saved baseline never changes. Entries/digest/read stay unavailable
 * until every path and full file agrees again; any failure is sticky.
 * Caller still holds original isolation/cuts. This grants no graph authority. */
extern ClusterRecoveryRootsResult
cluster_recovery_root_set_recheck_begin(ClusterRecoveryRootSet *roots);
extern uint32 cluster_recovery_root_set_count(const ClusterRecoveryRootSet *roots);
extern const ClusterRecoveryRootEntry *
cluster_recovery_root_set_entry(const ClusterRecoveryRootSet *roots, uint32 index);
/* Bind one canonical relation candidate to this observed namespace and find
 * its SPACE candidate. UINT32_MAX means no SPACE candidate, never permission
 * to ignore the relation. No I/O or authority acquisition. */
extern ClusterRecoveryRootsResult cluster_recovery_root_set_relation(ClusterRecoveryRootSet *roots,
																	 uint32 index,
																	 ClusterSpaceIdentityKey *key,
																	 uint32 *space_index);
/* Compare the observed namespace binding only; this is not ROOT authority. */
extern bool cluster_recovery_root_set_namespace_matches(const ClusterRecoveryRootSet *roots,
														uint64 system_identifier,
														uint64 database_incarnation);
/* Read one chunk of a physical file already in OBSERVED. Every path component
 * is reopened without symlink traversal and compared with its saved object;
 * file and path are rechecked after the exact read. Missing WAL-only objects,
 * short reads and namespace changes poison the observation. This is still a
 * candidate read: the cold owner retains isolation/cuts and performs its full
 * final graph revalidation. No terminal authority follows from stat equality. */
extern ClusterRecoveryRootsResult cluster_recovery_root_set_read(ClusterRecoveryRootSet *roots,
																 uint32 index, uint64 offset,
																 void *buffer, size_t length);
/* Full SHA-256 of a domain-separated, canonical path-ordered encoding.
 * No stat padding, truncated hash or CRC. Only available after OBSERVED;
 * digest equality is an index, never a substitute for the original proofs. */
extern bool cluster_recovery_root_set_digest(const ClusterRecoveryRootSet *roots, uint8 out[32]);
extern void cluster_recovery_root_set_destroy(ClusterRecoveryRootSet *roots);

#endif
#endif
