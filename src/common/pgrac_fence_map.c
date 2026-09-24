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

/*
 * EVP's raw-key import/public_check does not reject small-order Ed25519 keys.
 * In particular A=identity accepts (R=identity,S=0) without a private key.
 * Reject all canonical small-order y coordinates and y >= 2^255-19; x's sign
 * cannot make these safe. These are public values, not secret comparisons.
 *
 * Encoding: RFC 8032 section 5.1.3. Order-eight coordinates can also be checked
 * against libsodium 1.0.18 ge25519_has_small_order. This is only an encoding/
 * small-order prefilter, not a replacement curve or full subgroup validation.
 */
bool
pgrac_fence_ed25519_key_acceptable(const uint8 key[32])
{
	static const uint8 order_eight_y[2][32]
		= { { 0x26, 0xe8, 0x95, 0x8f, 0xc2, 0xb2, 0x27, 0xb0, 0x45, 0xc3, 0xf4,
			  0x89, 0xf2, 0xef, 0x98, 0xf0, 0xd5, 0xdf, 0xac, 0x05, 0xd3, 0xc6,
			  0x33, 0x39, 0xb1, 0x38, 0x02, 0x88, 0x6d, 0x53, 0xfc, 0x05 },
			{ 0xc7, 0x17, 0x6a, 0x70, 0x3d, 0x4d, 0xd8, 0x4f, 0xba, 0x3c, 0x0b,
			  0x76, 0x0d, 0x10, 0x67, 0x0f, 0x2a, 0x20, 0x53, 0xfa, 0x2c, 0x39,
			  0xcc, 0xc6, 0x4e, 0xc7, 0xfd, 0x77, 0x92, 0xac, 0x03, 0x7a } };
	uint8 y[32], low[32] = { 0 }, upper[32];

	if (key == NULL)
		return false;
	memcpy(y, key, sizeof(y));
	y[31] &= 0x7f;
	if (memcmp(y, low, sizeof(y)) == 0)
		return false; /* order four */
	low[0] = 1;
	if (memcmp(y, low, sizeof(y)) == 0)
		return false; /* identity */
	memset(upper, 0xff, sizeof(upper));
	upper[31] = 0x7f;
	if (memcmp(y + 1, upper + 1, 31) == 0 && y[0] >= 0xec)
		return false; /* p-1 (order two), p through 2^255-1 (noncanonical) */
	for (size_t i = 0; i < lengthof(order_eight_y); ++i)
		if (memcmp(y, order_eight_y[i], sizeof(y)) == 0)
			return false;
	return true;
}

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
	if (!pgrac_fence_ed25519_key_acceptable(trusted_key))
		return PGRAC_FENCE_MAP_BAD_SIGNATURE;
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
