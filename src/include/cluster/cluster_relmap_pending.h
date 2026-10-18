/*-------------------------------------------------------------------------
 *
 * cluster_relmap_pending.h
 *    Bounded structural pending obligations for relmap DROP/TRUNCATE.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 * Spec: 2026-10-14-pre2-m5-e1-format-closure-v1.md
 *
 * This is a PGRAC adaptation.  It is a pure in-memory contract and has no
 * live allocator, cold-owner, recovery-graph or filesystem caller yet.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RELMAP_PENDING_H
#define CLUSTER_RELMAP_PENDING_H

#include "cluster/cluster_space_identity.h"
#include "common/cluster_relmap_v2.h"

#ifdef USE_PGRAC_CLUSTER

#define CLUSTER_RELMAP_PENDING_CAPACITY 64

typedef enum ClusterRelmapPendingKind
{
	CLUSTER_RELMAP_PENDING_INVALID = 0,
	CLUSTER_RELMAP_PENDING_DROP = 1,
	CLUSTER_RELMAP_PENDING_TRUNCATE = 2
} ClusterRelmapPendingKind;

typedef enum ClusterRelmapPendingResult
{
	CLUSTER_RELMAP_PENDING_OK = 0,
	CLUSTER_RELMAP_PENDING_INVALID_INPUT,
	CLUSTER_RELMAP_PENDING_CAPACITY_EXCEEDED,
	CLUSTER_RELMAP_PENDING_CONFLICT,
	CLUSTER_RELMAP_PENDING_NOT_FOUND,
	CLUSTER_RELMAP_PENDING_NOT_READY
} ClusterRelmapPendingResult;

/*
 * The entry remains occupied until all proof bits are supplied.  The MAIN
 * placeholder and occupied relNumber are structural state, not a proof that
 * either physical copy has been cleared.
 */
typedef struct ClusterRelmapPendingEntry
{
	ClusterSpaceIdentityKey identity;
	ClusterRelmapPendingKind kind;
	bool main_placeholder;
	bool copies_cleared_readback;
	bool native_terminal;
	bool tt_complete;
	bool space_complete;
	bool occupied;
} ClusterRelmapPendingEntry;

/* One namespace per set; callers own synchronization around this state. */
typedef struct ClusterRelmapPendingSet
{
	bool has_namespace;
	uint64 system_identifier;
	uint64 database_incarnation;
	ClusterRelmapPendingEntry entries[CLUSTER_RELMAP_PENDING_CAPACITY];
} ClusterRelmapPendingSet;

/*
 * Structural pending records are replayed from the exact cold-recovery
 * stream.  They are not inferred from the opaque v2 pending RelMapFile.
 */
typedef struct ClusterRelmapPendingDescriptor
{
	ClusterRelmapPendingKind kind;
	ClusterSpaceIdentityKey identity;
} ClusterRelmapPendingDescriptor;

extern void cluster_relmap_pending_init(ClusterRelmapPendingSet *set);

/*
 * Rebuild an unproven structural directory from a committed-only v2 reader
 * result.  The caller must pass the exact pair result and view returned by
 * cluster_relmap_v2_read_pair (or its authority wrapper).  A degraded or
 * incomplete pair is never treated as an empty directory.  The v2 pending
 * image is deliberately ignored: structural DROP/TRUNCATE descriptors come
 * from the exact cold-recovery stream, and every resulting entry starts with
 * all terminal proof bits clear.
 *
 * The destination must be freshly initialized and empty.  On any failure it
 * is left unchanged.
 */
extern ClusterRelmapPendingResult cluster_relmap_pending_rebuild_from_committed(
	ClusterRelmapPendingSet *set, const ClusterRelmapV2Namespace *expected,
	ClusterRelmapV2PairResult pair_result,
	const ClusterRelmapV2View *view,
	const ClusterRelmapPendingDescriptor *descriptors, size_t descriptor_count);

/*
 * Add a DROP/TRUNCATE obligation.  MAIN must already be represented by the
 * caller's placeholder.  A locator collision, including a changed storage
 * UUID or operation kind, is rejected so relNumber cannot be reused while an
 * entry is retained.
 */
extern ClusterRelmapPendingResult cluster_relmap_pending_add(
	ClusterRelmapPendingSet *set, ClusterRelmapPendingKind kind,
	const ClusterSpaceIdentityKey *identity, bool main_placeholder);

extern bool cluster_relmap_pending_contains(const ClusterRelmapPendingSet *set,
											ClusterRelmapPendingKind kind,
											const ClusterSpaceIdentityKey *identity);

/* True for any exact namespace/locator entry, even if a new storage UUID is
 * proposed.  Allocator/relmap integration must consult this before reuse. */
extern bool cluster_relmap_pending_rel_number_reserved(
	const ClusterRelmapPendingSet *set, const ClusterSpaceIdentityKey *identity);

/* Each mark represents a caller-owned proof.  There is intentionally no
 * one-sided-copy, process-exit, ERROR, or empty-directory release operation. */
extern ClusterRelmapPendingResult cluster_relmap_pending_mark_copies_cleared(
	ClusterRelmapPendingSet *set, ClusterRelmapPendingKind kind,
	const ClusterSpaceIdentityKey *identity);
extern ClusterRelmapPendingResult cluster_relmap_pending_mark_native_terminal(
	ClusterRelmapPendingSet *set, ClusterRelmapPendingKind kind,
	const ClusterSpaceIdentityKey *identity);
extern ClusterRelmapPendingResult cluster_relmap_pending_mark_tt_complete(
	ClusterRelmapPendingSet *set, ClusterRelmapPendingKind kind,
	const ClusterSpaceIdentityKey *identity);
extern ClusterRelmapPendingResult cluster_relmap_pending_mark_space_complete(
	ClusterRelmapPendingSet *set, ClusterRelmapPendingKind kind,
	const ClusterSpaceIdentityKey *identity);

extern bool cluster_relmap_pending_can_release(const ClusterRelmapPendingSet *set,
											ClusterRelmapPendingKind kind,
											const ClusterSpaceIdentityKey *identity);

/* Release is the only operation that clears the reservation, and it succeeds
 * only after both-copy readback plus native/TT/SPACE completion proofs. */
extern ClusterRelmapPendingResult cluster_relmap_pending_release(
	ClusterRelmapPendingSet *set, ClusterRelmapPendingKind kind,
	const ClusterSpaceIdentityKey *identity);

#endif /* USE_PGRAC_CLUSTER */
#endif /* CLUSTER_RELMAP_PENDING_H */
