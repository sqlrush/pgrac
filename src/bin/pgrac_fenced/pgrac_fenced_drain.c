/*-------------------------------------------------------------------------
 *
 * pgrac_fenced_drain.c
 *    Validate exact route completion before constructing a drained readback.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_drain.c
 *
 * NOTES
 *    PGRAC-original consumer. Authentication and physical drain are external
 *    prerequisites; neither a timeout nor an empty session list is evidence.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"

#include "pgrac_fenced_drain.h"

static bool
present(const uint8 *bytes, size_t length)
{
	size_t i;

	for (i = 0; i < length; i++) {
		if (bytes[i] != 0)
			return true;
	}
	return false;
}

static bool
expectation_valid(const PgracFencedDrainIdentityV1 *identity)
{
	return identity != NULL && identity->attempt != 0 && identity->mapping_generation != 0
		   && identity->route_count > 0
		   && identity->route_count <= PGRAC_PROTECTED_SET_V2_MAX_ROUTES
		   && present(identity->operation_id, sizeof(identity->operation_id))
		   && present(identity->daemon_boot_id, sizeof(identity->daemon_boot_id))
		   && present(identity->target_boot_id, sizeof(identity->target_boot_id))
		   && present(identity->challenge, sizeof(identity->challenge))
		   && present(identity->guest_uuid, sizeof(identity->guest_uuid))
		   && present(identity->protected_set_digest, sizeof(identity->protected_set_digest));
}

static bool
identity_matches(const PgracFencedDrainIdentityV1 *a, const PgracFencedDrainIdentityV1 *b)
{
	return a->attempt == b->attempt && a->mapping_generation == b->mapping_generation
		   && a->route_count == b->route_count
		   && memcmp(a->operation_id, b->operation_id, sizeof(a->operation_id)) == 0
		   && memcmp(a->daemon_boot_id, b->daemon_boot_id, sizeof(a->daemon_boot_id)) == 0
		   && memcmp(a->target_boot_id, b->target_boot_id, sizeof(a->target_boot_id)) == 0
		   && memcmp(a->challenge, b->challenge, sizeof(a->challenge)) == 0
		   && memcmp(a->guest_uuid, b->guest_uuid, sizeof(a->guest_uuid)) == 0
		   && memcmp(a->protected_set_digest, b->protected_set_digest,
					 sizeof(a->protected_set_digest))
				  == 0;
}

static PgracFencedDrainResult
routes_complete(const PgracFencedDrainEvidenceV1 *observed, uint32 expected_count)
{
	bool seen[PGRAC_PROTECTED_SET_V2_MAX_ROUTES] = { false };
	bool complete = true;
	uint32 i;

	/* Reject surplus entries before dereferencing the supplied route array. */
	if (observed->route_count > expected_count)
		return PGRAC_DRAIN_MALFORMED;
	if (observed->route_count < expected_count)
		return PGRAC_DRAIN_INCOMPLETE;
	if (observed->routes == NULL)
		return PGRAC_DRAIN_MALFORMED;
	for (i = 0; i < observed->route_count; i++) {
		const PgracFencedDrainRouteV1 *route = &observed->routes[i];

		if (route->ordinal >= expected_count || seen[route->ordinal]
			|| (route->completed & ~PGRAC_DRAIN_ROUTE_COMPLETE) != 0)
			return PGRAC_DRAIN_MALFORMED;
		seen[route->ordinal] = true;
		complete = complete && route->completed == PGRAC_DRAIN_ROUTE_COMPLETE;
	}
	return complete ? PGRAC_DRAIN_PROVEN : PGRAC_DRAIN_INCOMPLETE;
}

PgracFencedDrainResult
pgrac_fenced_drain_verify(const PgracFencedDrainIdentityV1 *expected,
						  const PgracFencedDrainEvidenceV1 *observed, PgracFencedReadbackV1 *out)
{
	PgracFencedDrainResult result;

	if (out == NULL)
		return PGRAC_DRAIN_BAD_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (!expectation_valid(expected) || observed == NULL)
		return PGRAC_DRAIN_BAD_ARGUMENT;
	if (!identity_matches(expected, &observed->identity))
		return PGRAC_DRAIN_IDENTITY_MISMATCH;
	if (observed->target_state < PGRAC_FENCED_TARGET_OFF
		|| observed->target_state > PGRAC_FENCED_TARGET_UNKNOWN
		|| (observed->completed & ~PGRAC_DRAIN_GLOBAL_COMPLETE) != 0)
		return PGRAC_DRAIN_MALFORMED;
	if (observed->target_state != PGRAC_FENCED_TARGET_OFF)
		return PGRAC_DRAIN_TARGET_NOT_OFF;
	result = routes_complete(observed, expected->route_count);
	if (result != PGRAC_DRAIN_PROVEN)
		return result;
	if (observed->completed != PGRAC_DRAIN_GLOBAL_COMPLETE)
		return PGRAC_DRAIN_INCOMPLETE;
	out->state = PGRAC_FENCED_TARGET_OFF;
	out->io_drain_state = PGRAC_FENCED_IO_DRAIN_DRAINED;
	memcpy(out->observed_target_uuid, expected->guest_uuid, sizeof(out->observed_target_uuid));
	return PGRAC_DRAIN_PROVEN;
}
