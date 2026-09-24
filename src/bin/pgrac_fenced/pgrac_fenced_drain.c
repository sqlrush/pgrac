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

#include "common/pgrac_fence_map.h"
#include "pgrac_fenced_drain.h"

#ifdef USE_OPENSSL
#include <openssl/evp.h>
#endif

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

static uint32
frame_u32(const uint8 *bytes)
{
	return (uint32)bytes[0] | ((uint32)bytes[1] << 8) | ((uint32)bytes[2] << 16)
		   | ((uint32)bytes[3] << 24);
}

static uint64
frame_u64(const uint8 *bytes)
{
	return (uint64)frame_u32(bytes) | ((uint64)frame_u32(bytes + 4) << 32);
}

static PgracFencedDrainResult
frame_signature(const uint8 *bytes, size_t signed_length, const uint8 trusted_key[32])
{
#if defined(USE_OPENSSL) && defined(EVP_PKEY_ED25519)
	EVP_PKEY *key = EVP_PKEY_new_raw_public_key(EVP_PKEY_ED25519, NULL, trusted_key, 32);
	EVP_MD_CTX *context = EVP_MD_CTX_new();
	PgracFencedDrainResult result = PGRAC_DRAIN_UNSUPPORTED;

	if (key != NULL && context != NULL && EVP_DigestVerifyInit(context, NULL, NULL, NULL, key) == 1)
		result = EVP_DigestVerify(context, bytes + signed_length, PGRAC_DRAIN_FRAME_SIGNATURE_BYTES,
								  bytes, signed_length)
						 == 1
					 ? PGRAC_DRAIN_PROVEN
					 : PGRAC_DRAIN_BAD_SIGNATURE;
	EVP_MD_CTX_free(context);
	EVP_PKEY_free(key);
	return result;
#else
	(void)bytes;
	(void)signed_length;
	(void)trusted_key;
	return PGRAC_DRAIN_UNSUPPORTED;
#endif
}

/* Authentication precedes parsing facts; authenticated facts still require all obligations. */
PgracFencedDrainResult
pgrac_fenced_drain_verify_frame(const PgracFencedDrainIdentityV1 *expected,
								const uint8 trusted_key[32], const uint8 *bytes, size_t length,
								PgracFencedReadbackV1 *out)
{
	PgracFencedDrainEvidenceV1 observed;
	PgracFencedDrainRouteV1 routes[PGRAC_PROTECTED_SET_V2_MAX_ROUTES];
	PgracFencedDrainResult result;
	uint32 count;
	uint32 i;

	if (out == NULL)
		return PGRAC_DRAIN_BAD_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (!expectation_valid(expected) || trusted_key == NULL || bytes == NULL
		|| !present(trusted_key, 32))
		return PGRAC_DRAIN_BAD_ARGUMENT;
	if (length < PGRAC_DRAIN_FRAME_HEADER_BYTES + PGRAC_DRAIN_FRAME_SIGNATURE_BYTES
		|| length > PGRAC_DRAIN_FRAME_MAX_BYTES || memcmp(bytes, "PGRDRN01", 8) != 0
		|| bytes[8] != 1 || bytes[9] != 0 || bytes[10] != PGRAC_DRAIN_FRAME_HEADER_BYTES
		|| bytes[11] != 0 || frame_u32(bytes + 12) != length || present(bytes + 160, 16))
		return PGRAC_DRAIN_MALFORMED;
	count = frame_u32(bytes + 156);
	if (count == 0 || count > PGRAC_PROTECTED_SET_V2_MAX_ROUTES
		|| length != PGRAC_DRAIN_FRAME_HEADER_BYTES + 8 * count + PGRAC_DRAIN_FRAME_SIGNATURE_BYTES)
		return PGRAC_DRAIN_MALFORMED;
	if (!pgrac_fence_ed25519_key_acceptable(trusted_key))
		return PGRAC_DRAIN_BAD_SIGNATURE;
	result = frame_signature(bytes, length - PGRAC_DRAIN_FRAME_SIGNATURE_BYTES, trusted_key);
	if (result != PGRAC_DRAIN_PROVEN)
		return result;
	memset(&observed, 0, sizeof(observed));
	memcpy(observed.identity.operation_id, bytes + 16, 16);
	observed.identity.attempt = frame_u64(bytes + 32);
	memcpy(observed.identity.daemon_boot_id, bytes + 40, 16);
	memcpy(observed.identity.target_boot_id, bytes + 56, 16);
	memcpy(observed.identity.challenge, bytes + 72, 16);
	memcpy(observed.identity.guest_uuid, bytes + 88, 16);
	observed.identity.mapping_generation = frame_u64(bytes + 104);
	memcpy(observed.identity.protected_set_digest, bytes + 112, 32);
	observed.identity.route_count = frame_u32(bytes + 144);
	observed.target_state = frame_u32(bytes + 148);
	observed.completed = frame_u32(bytes + 152);
	observed.route_count = count;
	observed.routes = routes;
	for (i = 0; i < count; ++i) {
		routes[i].ordinal = frame_u32(bytes + PGRAC_DRAIN_FRAME_HEADER_BYTES + i * 8);
		routes[i].completed = frame_u32(bytes + PGRAC_DRAIN_FRAME_HEADER_BYTES + i * 8 + 4);
	}
	return pgrac_fenced_drain_verify(expected, &observed, out);
}
