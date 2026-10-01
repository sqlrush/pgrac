/*-------------------------------------------------------------------------
 * cluster_space_reservation.h
 *    Canonical main-fork sequential reservations in SPACE block one.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SPACE_RESERVATION_H
#define CLUSTER_SPACE_RESERVATION_H

#include "cluster/cluster_space_identity.h"

#ifdef USE_PGRAC_CLUSTER

#define CLUSTER_SPACE_RESERVATION_PROFILE 2
#define CLUSTER_SPACE_RESERVATION_BLOCK 1
#define CLUSTER_SPACE_RESERVATION_BYTES 160
#define CLUSTER_SPACE_RESERVATION_MAGIC UINT32_C(0x31525350)
#define CLUSTER_SPACE_RESERVATION_FORMAT 1
#define CLUSTER_SPACE_RESERVATION_WAL_BYTES 368
#define CLUSTER_SPACE_RESERVATION_WAL_MAGIC UINT32_C(0x31565350)
#define CLUSTER_SPACE_STRUCTURE_WAL_BYTES (CLUSTER_SPACE_WAL_BYTES + CLUSTER_SPACE_RESERVATION_WAL_BYTES)

/* In-memory fields, never copied as a disk/wire structure. InvalidBlockNumber
 * is a valid exhausted exclusive upper bound, not an allocatable block. */
typedef struct ClusterSpaceReservation {
	ClusterSpaceIdentity identity;
	BlockNumber next_block;
} ClusterSpaceReservation;

typedef enum ClusterSpaceReservationAction {
	CLUSTER_SPACE_RESERVATION_INIT = 1,
	CLUSTER_SPACE_RESERVATION_ADVANCE = 2,
	CLUSTER_SPACE_RESERVATION_RESET = 3,
	CLUSTER_SPACE_RESERVATION_TOMBSTONE = 4
} ClusterSpaceReservationAction;

typedef struct ClusterSpaceReservationChange {
	ClusterSpaceReservationAction action;
	BlockNumber first_block;
	uint32 granted;
	uint64 before_token;
	uint64 result_token;
	ClusterSpaceReservation before;
	ClusterSpaceReservation result;
} ClusterSpaceReservationChange;

typedef struct ClusterSpaceStructureChange {
	ClusterSpaceWalChange identity;
	ClusterSpaceReservationChange reservation;
} ClusterSpaceStructureChange;

/* Pure representation and byte transitions only. No allocation, authority,
 * I/O, WAL insertion, grant, durability or retention decision. Every refusal
 * leaves output unchanged. Decode accepts unaligned input. */
extern bool cluster_space_reservation_encode(const ClusterSpaceReservation *state,
											  void *bytes, size_t length);
extern bool cluster_space_reservation_decode(const void *bytes, size_t length,
											  const ClusterSpaceIdentityKey *expected,
											  ClusterSpaceReservation *out);
extern bool cluster_space_reservation_page_encode(const ClusterSpaceReservation *state,
												   uint64 token, void *page, size_t length);
extern bool cluster_space_reservation_page_decode(const void *page, size_t length,
												   ForkNumber forknum, BlockNumber block,
												   const ClusterSpaceIdentityKey *expected,
												   ClusterSpaceReservation *out, uint64 *token);
extern bool cluster_space_reservation_page_valid(const void *page, size_t length);
extern bool cluster_space_reservation_wal_encode(const ClusterSpaceReservationChange *change,
												  void *bytes, size_t length);
extern bool cluster_space_reservation_wal_decode(const void *bytes, size_t length,
												  ClusterSpaceReservationChange *out);

/* RESET/TOMBSTONE require their original structural owner and all-component
 * preflight; this helper never truncates/drops a relation. ALREADY requires
 * identical full typed result, not numeric ordering. Caller stamps WAL LSN/
 * origin on APPLY and owns write/fsync/post-read before recovery readiness. */
extern ClusterSpaceIdentityTransition
cluster_space_reservation_apply(const ClusterSpaceReservationChange *change,
								const ClusterSpaceIdentityKey *expected,
								void *page, size_t length);

/* One structural record binds both independently versioned SPACE pages.
 * Both private images must pass before either output changes. Exact result
 * on just one component is a restartable partial installation, not permission
 * to skip the other component or the original owner's physical action.
 * apply_mask bit0/bit1 identifies changed pages; output is untouched on refusal. */
extern bool cluster_space_structure_wal_encode(const ClusterSpaceStructureChange *change,
											void *bytes, size_t length);
extern bool cluster_space_structure_wal_decode(const void *bytes, size_t length,
											ClusterSpaceStructureChange *out);
extern ClusterSpaceIdentityTransition
cluster_space_structure_apply(const ClusterSpaceStructureChange *change,
							  const ClusterSpaceIdentityKey *expected,
							  void *identity_page, void *reservation_page,
							  size_t length, uint8 *apply_mask);

/* Closed retained inputs for one exact namespace/locator. The caller binds
 * each payload to its real WAL owner (in particular COMMIT for TOMBSTONE).
 * This representation carries no source, lifecycle or mutation authority. */
typedef struct ClusterSpaceRecoveryInput {
	const void *data;
	size_t length;
} ClusterSpaceRecoveryInput;

typedef struct ClusterSpaceRecoveryImage {
	PGAlignedBlock pages[2];
	/* Input responsible for each final page, UINT32_MAX if unchanged by WAL. */
	uint32 source_index[2];
	uint8 apply_mask;
	/* Existing canonical target covers omitted predecessor reservations.
	 * Caller must prove these original target pages durable before publishing
	 * any coverage. This is not permission to write unknown successor WAL. */
	uint8 covered_by_successor_mask;
} ClusterSpaceRecoveryImage;

/* Validate a complete, nonbranching structural chain, then prepare both pages.
 * order has count entries and preserves every structural action, even when
 * both pages already match the final result. ADVANCE-only input permits
 * disjoint monotone ranges inside one exact LIVE identity. The observed
 * target may cover earlier ranges; the remaining suffix must begin at its
 * exact token/state. Such successor coverage is explicit in the output and
 * still requires the original owner's physical durability qualification.
 * No numeric token/LSN ordering is evidence. All outputs stay intact on refusal.
 * The original owner still performs physical actions, WAL/source revalidation,
 * write/fsync/post-read and contribution publication under its protected set. */
extern bool cluster_space_recovery_prepare(const ClusterSpaceRecoveryInput *inputs,
										  uint32 count, const ClusterSpaceIdentityKey *expected,
										  const void *identity_page, const void *reservation_page,
										  uint32 *order, ClusterSpaceRecoveryImage *out);
/* Heap scratch used by preparation, or zero for an unrepresentable size. */
extern size_t cluster_space_recovery_scratch_bytes(uint32 count);

/* Validate and order retained inputs (a full structural chain, or disjoint
 * ADVANCE-only ranges in one LIVE identity). No target is
 * read or certified, no page image or mutation permission is returned. The
 * same input checks are also mandatory in prepare with the real target. */
extern bool cluster_space_recovery_order(const ClusterSpaceRecoveryInput *inputs, uint32 count,
										 const ClusterSpaceIdentityKey *expected, uint32 *order);

#endif
#endif
