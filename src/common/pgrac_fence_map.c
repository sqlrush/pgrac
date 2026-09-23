/*-------------------------------------------------------------------------
 *
 * pgrac_fence_map.c
 *    Verify a signed protected-set configuration against pinned expectations.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * IDENTIFICATION
 *    src/common/pgrac_fence_map.c
 *
 * NOTES
 *    PGRAC-original mapping verifier; it grants no recovery/write authority.
 *    Private keys are never loaded by this frontend/backend consumer.
 *
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "common/pgrac_fence_map.h"

#ifdef USE_OPENSSL
#include <openssl/evp.h>
#endif

static bool
bytes_present(const uint8 *bytes, size_t length)
{
	size_t i;

	if (bytes == NULL)
		return false;
	for (i = 0; i < length; i++) {
		if (bytes[i] != 0)
			return true;
	}
	return false;
}

static uint16
map_u16(const uint8 *bytes)
{
	return (uint16)bytes[0] | ((uint16)bytes[1] << 8);
}

static uint32
map_u32(const uint8 *bytes)
{
	return (uint32)bytes[0] | ((uint32)bytes[1] << 8) | ((uint32)bytes[2] << 16)
		   | ((uint32)bytes[3] << 24);
}

static uint64
map_u64(const uint8 *bytes)
{
	return (uint64)map_u32(bytes) | ((uint64)map_u32(bytes + 4) << 32);
}

static bool
map_header_valid(const uint8 *bytes, size_t length, uint32 *payload_length)
{
	uint32 size;

	if (length < PGRAC_FENCE_MAP_V2_HEADER_BYTES + PGRAC_FENCE_MAP_V2_SIGNATURE_BYTES
		|| length > PGRAC_FENCE_MAP_V2_MAX_BYTES)
		return false;
	if (memcmp(bytes, "PGRFMAP2", 8) != 0 || map_u16(bytes + 8) != 2
		|| map_u16(bytes + 10) != PGRAC_FENCE_MAP_V2_HEADER_BYTES
		|| map_u32(bytes + 12) >= PGRAC_FENCE_MAP_V2_MAX_NODES || map_u64(bytes + 16) == 0
		|| map_u32(bytes + 28) != 0)
		return false;
	size = map_u32(bytes + 24);
	if (size == 0 || size > PGRAC_PROTECTED_SET_V2_MAX_BYTES
		|| length != PGRAC_FENCE_MAP_V2_HEADER_BYTES + size + PGRAC_FENCE_MAP_V2_SIGNATURE_BYTES)
		return false;
	*payload_length = size;
	return true;
}

static PgracFenceMapResult
verify_signature(const uint8 *bytes, size_t signed_length, const uint8 trusted_key[32])
{
#if defined(USE_OPENSSL) && defined(EVP_PKEY_ED25519)
	EVP_PKEY *key;
	EVP_MD_CTX *context;
	PgracFenceMapResult result = PGRAC_FENCE_MAP_UNSUPPORTED;
	int verified;

	key = EVP_PKEY_new_raw_public_key(EVP_PKEY_ED25519, NULL, trusted_key, 32);
	if (key == NULL)
		return result;
	context = EVP_MD_CTX_new();
	if (context != NULL && EVP_DigestVerifyInit(context, NULL, NULL, NULL, key) == 1) {
		/* Pure Ed25519 verifies the complete message, without an external digest. */
		verified = EVP_DigestVerify(context, bytes + signed_length,
									PGRAC_FENCE_MAP_V2_SIGNATURE_BYTES, bytes, signed_length);
		if (verified == 1)
			result = PGRAC_FENCE_MAP_OK;
		else if (verified == 0)
			result = PGRAC_FENCE_MAP_BAD_SIGNATURE;
	}
	EVP_MD_CTX_free(context);
	EVP_PKEY_free(key);
	return result;
#else
	return PGRAC_FENCE_MAP_UNSUPPORTED;
#endif
}

PgracFenceMapResult
pgrac_fence_map_v2_verify(const uint8 *bytes, size_t length, const uint8 trusted_key[32],
						  const PgracFenceMapExpectedV2 *expected, PgracProtectedSetDecodedV2 *out)
{
	PgracFenceMapResult result;
	uint32 payload_length;
	uint8 digest[PGRAC_PROTECTED_SET_DIGEST_BYTES];

	if (out == NULL)
		return PGRAC_FENCE_MAP_BAD_ARGUMENT;
	memset(out, 0, sizeof(*out));
	if (bytes == NULL || expected == NULL || expected->system_identifier == 0
		|| expected->victim_node_id >= PGRAC_FENCE_MAP_V2_MAX_NODES
		|| expected->mapping_generation == 0
		|| !bytes_present(expected->protected_set_digest, sizeof(expected->protected_set_digest))
		|| !bytes_present(trusted_key, PGRAC_FENCE_MAP_V2_PUBLIC_KEY_BYTES))
		return PGRAC_FENCE_MAP_BAD_ARGUMENT;
	if (!map_header_valid(bytes, length, &payload_length))
		return PGRAC_FENCE_MAP_BAD_FORMAT;
	result = verify_signature(bytes, PGRAC_FENCE_MAP_V2_HEADER_BYTES + payload_length, trusted_key);
	if (result != PGRAC_FENCE_MAP_OK)
		return result;
	if (!pgrac_protected_set_v2_decode(bytes + PGRAC_FENCE_MAP_V2_HEADER_BYTES, payload_length,
									   out))
		return PGRAC_FENCE_MAP_BAD_FORMAT;
	if (map_u64(bytes + 16) != expected->system_identifier
		|| map_u32(bytes + 12) != expected->victim_node_id
		|| out->set.mapping_generation != expected->mapping_generation
		|| !pgrac_external_fence_protected_set_digest_v2(&out->set, digest)
		|| memcmp(digest, expected->protected_set_digest, sizeof(digest)) != 0) {
		memset(out, 0, sizeof(*out));
		return PGRAC_FENCE_MAP_WRONG_BINDING;
	}
	return PGRAC_FENCE_MAP_OK;
}
