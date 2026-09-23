/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_drain.h
 *    Exact-operation validation of target-side storage completion evidence.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_drain.h
 *
 * NOTES
 *    PGRAC-original in-memory contract, not a wire or authentication format.
 *    The caller authenticates the helper and independently binds expectations.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCED_DRAIN_H
#define PGRAC_FENCED_DRAIN_H

#include "common/pgrac_protected_set.h"
#include "pgrac_fenced_provider.h"

#define PGRAC_DRAIN_DENY_DURABLE UINT32_C(1)
#define PGRAC_DRAIN_ADMISSION_DISABLED UINT32_C(2)
#define PGRAC_DRAIN_SESSION_STOPPED UINT32_C(4)
#define PGRAC_DRAIN_COMMANDS_COMPLETED UINT32_C(8)
#define PGRAC_DRAIN_BACKSTORE_FLUSHED UINT32_C(16)
#define PGRAC_DRAIN_ROUTE_COMPLETE UINT32_C(31)

#define PGRAC_DRAIN_INVENTORY_COMPLETE UINT32_C(1)
#define PGRAC_DRAIN_UNSOLICITED_ON_BLOCKED UINT32_C(2)
#define PGRAC_DRAIN_GLOBAL_COMPLETE UINT32_C(3)

typedef struct PgracFencedDrainIdentityV1 {
	uint8 operation_id[16];
	uint64 attempt;
	uint8 daemon_boot_id[16];
	uint8 target_boot_id[16];
	uint8 challenge[16];
	uint8 guest_uuid[16];
	uint64 mapping_generation;
	uint8 protected_set_digest[32];
	uint32 route_count;
} PgracFencedDrainIdentityV1;

typedef struct PgracFencedDrainRouteV1 {
	/* Ordinal in the authenticated canonical protected-set map. */
	uint32 ordinal;
	uint32 completed;
} PgracFencedDrainRouteV1;

typedef struct PgracFencedDrainEvidenceV1 {
	PgracFencedDrainIdentityV1 identity;
	uint32 target_state;
	uint32 completed;
	uint32 route_count;
	const PgracFencedDrainRouteV1 *routes;
} PgracFencedDrainEvidenceV1;

typedef enum PgracFencedDrainResult {
	PGRAC_DRAIN_PROVEN = 0,
	PGRAC_DRAIN_BAD_ARGUMENT,
	PGRAC_DRAIN_IDENTITY_MISMATCH,
	PGRAC_DRAIN_MALFORMED,
	PGRAC_DRAIN_TARGET_NOT_OFF,
	PGRAC_DRAIN_INCOMPLETE
} PgracFencedDrainResult;

/* No aliasing. Out is zero unless every current, authenticated obligation holds. */
extern PgracFencedDrainResult pgrac_fenced_drain_verify(const PgracFencedDrainIdentityV1 *expected,
														const PgracFencedDrainEvidenceV1 *observed,
														PgracFencedReadbackV1 *out);

#endif /* PGRAC_FENCED_DRAIN_H */
