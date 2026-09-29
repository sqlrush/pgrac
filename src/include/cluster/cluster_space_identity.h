/*-------------------------------------------------------------------------
 *
 * cluster_space_identity.h
 *    Persistent relation identity in the SPACE fork.
 *
 * Portions Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *    src/include/cluster/cluster_space_identity.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SPACE_IDENTITY_H
#define CLUSTER_SPACE_IDENTITY_H

#include "c.h"
#include "storage/block.h"
#include "storage/relfilelocator.h"

#ifdef USE_PGRAC_CLUSTER

#define CLUSTER_SPACE_IDENTITY_BYTES 128
#define CLUSTER_SPACE_IDENTITY_MAGIC UINT32_C(0x31495350)
#define CLUSTER_SPACE_IDENTITY_FORMAT 1
#define CLUSTER_SPACE_IDENTITY_PROFILE 1

typedef struct ClusterSpaceIdentityKey {
	uint64 system_identifier;
	uint64 database_incarnation;
	uint8 storage_uuid[16];
	RelFileLocator locator;
} ClusterSpaceIdentityKey;

typedef enum ClusterSpaceIdentityState {
	CLUSTER_SPACE_IDENTITY_LIVE = 1,
	CLUSTER_SPACE_IDENTITY_TOMBSTONED = 2
} ClusterSpaceIdentityState;

/* In-memory values only. Never serialize or compare this structure as bytes. */
typedef struct ClusterSpaceIdentity {
	ClusterSpaceIdentityKey key;
	uint8 incarnation[16];
	uint64 sequence;
	uint64 operation;
	ClusterSpaceIdentityState state;
} ClusterSpaceIdentity;

typedef enum ClusterSpaceIdentityTransition {
	CLUSTER_SPACE_IDENTITY_INVALID = 0,
	CLUSTER_SPACE_IDENTITY_APPLY,
	CLUSTER_SPACE_IDENTITY_ALREADY,
	CLUSTER_SPACE_IDENTITY_MISMATCH
} ClusterSpaceIdentityTransition;

/* No I/O, allocation, authority acquisition or output mutation on failure. */
extern bool cluster_space_identity_encode(const ClusterSpaceIdentity *identity, void *bytes,
										  size_t length);
extern bool cluster_space_identity_decode(const void *bytes, size_t length,
										  const ClusterSpaceIdentityKey *expected,
										  ClusterSpaceIdentity *out);
extern bool cluster_space_identity_page_encode(const ClusterSpaceIdentity *identity,
											   uint64 mutation_token, void *page, size_t length);
extern bool cluster_space_identity_page_decode(const void *page, size_t length, ForkNumber forknum,
											   BlockNumber block,
											   const ClusterSpaceIdentityKey *expected,
											   ClusterSpaceIdentity *out, uint64 *mutation_token);

/* Structural validity alone never grants identity, allocation or page access. */
extern bool cluster_space_identity_page_valid(const void *page, size_t length);
extern ClusterSpaceIdentityTransition
cluster_space_identity_transition(const ClusterSpaceIdentity *current,
								  const ClusterSpaceIdentity *expected,
								  const ClusterSpaceIdentity *result);

#endif /* USE_PGRAC_CLUSTER */
#endif /* CLUSTER_SPACE_IDENTITY_H */
