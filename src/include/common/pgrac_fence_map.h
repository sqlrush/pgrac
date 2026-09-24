/*-------------------------------------------------------------------------
 *
 * pgrac_fence_map.h
 *    Verification of a signed, current protected-set configuration.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/include/common/pgrac_fence_map.h
 *
 * NOTES
 *    PGRAC-original interface. Authentication is not isolation or admission.
 *    The trust key and current expectation must come from outside the packet.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PGRAC_FENCE_MAP_H
#define PGRAC_FENCE_MAP_H

#include "common/pgrac_protected_set.h"

#define PGRAC_FENCE_MAP_V2_HEADER_BYTES 32
#define PGRAC_FENCE_MAP_V2_SIGNATURE_BYTES 64
#define PGRAC_FENCE_MAP_V2_PUBLIC_KEY_BYTES 32
#define PGRAC_FENCE_MAP_V2_MAX_NODES 128
#define PGRAC_FENCE_MAP_V2_MAX_BYTES                                                               \
	(PGRAC_FENCE_MAP_V2_HEADER_BYTES + PGRAC_PROTECTED_SET_V2_MAX_BYTES                            \
	 + PGRAC_FENCE_MAP_V2_SIGNATURE_BYTES)

typedef enum PgracFenceMapResult {
	PGRAC_FENCE_MAP_OK = 0,
	PGRAC_FENCE_MAP_BAD_ARGUMENT,
	PGRAC_FENCE_MAP_BAD_FORMAT,
	PGRAC_FENCE_MAP_BAD_SIGNATURE,
	PGRAC_FENCE_MAP_WRONG_BINDING,
	PGRAC_FENCE_MAP_UNSUPPORTED
} PgracFenceMapResult;

typedef struct PgracFenceMapExpectedV2 {
	uint64 system_identifier;
	uint32 victim_node_id;
	uint64 mapping_generation;
	uint8 protected_set_digest[PGRAC_PROTECTED_SET_DIGEST_BYTES];
} PgracFenceMapExpectedV2;

/* Public-key prefilter only; EVP still performs point/signature verification. */
extern bool pgrac_fence_ed25519_key_acceptable(const uint8 key[32]);

/* No input/output aliasing. Successful text views borrow immutable packet bytes. */
extern PgracFenceMapResult
pgrac_fence_map_v2_verify(const uint8 *bytes, size_t length,
						  const uint8 trusted_key[PGRAC_FENCE_MAP_V2_PUBLIC_KEY_BYTES],
						  const PgracFenceMapExpectedV2 *expected, PgracProtectedSetDecodedV2 *out);

#endif /* PGRAC_FENCE_MAP_H */
