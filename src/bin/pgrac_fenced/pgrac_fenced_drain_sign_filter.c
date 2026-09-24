/*-------------------------------------------------------------------------
 * pgrac_fenced_drain_sign_filter.c
 *    Emit a signed bounded frame only after the common consumer accepts it.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 2026, pgrac contributors
 * IDENTIFICATION
 *    src/bin/pgrac_fenced/pgrac_fenced_drain_sign_filter.c
 * NOTES
 *    PGRAC-original. Only the target owner may supply evidence and the secret.
 *    This codec does not prove OFF, physical drain or future-ON exclusion.
 *-------------------------------------------------------------------------
 */
#include "postgres_fe.h"
#include "common/pgrac_fence_map.h"
#include "pgrac_fenced_drain.h"
#include "pgrac_fenced_drain_sign_filter.h"

#ifdef USE_OPENSSL
#include <openssl/crypto.h>
#include <openssl/evp.h>
#endif

static bool
public_key(const char *text, uint8 key[32])
{
	if (text == NULL || strlen(text) != 64)
		return false;
	for (size_t n = 0; n < 32; ++n) {
		unsigned value = 0;
		for (size_t digit = 0; digit < 2; ++digit) {
			unsigned char ch = text[n * 2 + digit];
			if (ch >= '0' && ch <= '9')
				value = value * 16 + ch - '0';
			else if (ch >= 'a' && ch <= 'f')
				value = value * 16 + ch - 'a' + 10;
			else
				return false;
		}
		key[n] = (uint8)value;
	}
	return true;
}

#if defined(USE_OPENSSL) && defined(EVP_PKEY_ED25519)
static uint64
number(const uint8 *at, size_t size)
{
	uint64 result = 0;
	for (size_t n = 0; n < size; ++n)
		result |= (uint64)at[n] << (n * 8);
	return result;
}
#endif

static bool
sign_checked(const uint8 seed[32], const uint8 pinned_key[32], uint8 *frame, size_t body)
{
#if defined(USE_OPENSSL) && defined(EVP_PKEY_ED25519)
	EVP_PKEY *key = NULL;
	EVP_MD_CTX *context = NULL;
	uint8 derived[32];
	size_t key_length = 32, signature_length = 64;
	PgracFencedDrainIdentityV1 identity;
	PgracFencedReadbackV1 readback;
	bool result = false;

	if (!pgrac_fence_ed25519_key_acceptable(pinned_key))
		return false;
	key = EVP_PKEY_new_raw_private_key(EVP_PKEY_ED25519, NULL, seed, 32);
	context = EVP_MD_CTX_new();
	if (key == NULL || context == NULL
		|| EVP_PKEY_get_raw_public_key(key, derived, &key_length) != 1 || key_length != 32
		|| CRYPTO_memcmp(derived, pinned_key, 32) != 0
		|| EVP_DigestSignInit(context, NULL, NULL, NULL, key) != 1
		|| EVP_DigestSign(context, frame + body, &signature_length, frame, body) != 1
		|| signature_length != 64)
		goto done;
	/*
	 * These are asserted facts of the restricted target owner, not independently
	 * authenticated input. The common consumer rejects malformed/incomplete
	 * assertions before any signature is released to that owner's pipe.
	 */
	memset(&identity, 0, sizeof(identity));
	memcpy(identity.operation_id, frame + 16, 16);
	identity.attempt = number(frame + 32, 8);
	memcpy(identity.daemon_boot_id, frame + 40, 16);
	memcpy(identity.target_boot_id, frame + 56, 16);
	memcpy(identity.challenge, frame + 72, 16);
	memcpy(identity.guest_uuid, frame + 88, 16);
	identity.mapping_generation = number(frame + 104, 8);
	memcpy(identity.protected_set_digest, frame + 112, 32);
	identity.route_count = (uint32)number(frame + 144, 4);
	result = pgrac_fenced_drain_verify_frame(&identity, pinned_key, frame, body + 64, &readback)
			 == PGRAC_DRAIN_PROVEN;
done:
	EVP_MD_CTX_free(context);
	EVP_PKEY_free(key);
	return result;
#else
	(void)seed;
	(void)pinned_key;
	(void)frame;
	(void)body;
	return false;
#endif
}

int
pgrac_fenced_drain_sign_filter(int argc, char *const *argv, FILE *input, FILE *output)
{
	uint8 incoming[32 + PGRAC_DRAIN_FRAME_MAX_BYTES - 64 + 1];
	uint8 frame[PGRAC_DRAIN_FRAME_MAX_BYTES];
	uint8 key[32];
	size_t length, body;
	int result = 77;

	if (argc != 2 || argv == NULL || input == NULL || output == NULL || !public_key(argv[1], key))
		return 2;
	length = fread(incoming, 1, sizeof(incoming), input);
	if (ferror(input) || !feof(input) || length >= sizeof(incoming)
		|| length < 32 + PGRAC_DRAIN_FRAME_HEADER_BYTES)
		goto done;
	body = length - 32;
	memcpy(frame, incoming + 32, body);
	if (!sign_checked(incoming, key, frame, body))
		goto done;
	if (fwrite(frame, 1, body + 64, output) != body + 64 || fflush(output) != 0 || ferror(output))
		goto done;
	result = 0;
done:
	explicit_bzero(incoming, sizeof(incoming));
	explicit_bzero(frame, sizeof(frame));
	return result;
}
