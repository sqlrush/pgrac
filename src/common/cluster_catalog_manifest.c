/*-------------------------------------------------------------------------
 * cluster_catalog_manifest.c
 *    Validate the complete original catalog publication-set image.
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef FRONTEND
#include "postgres.h"
#else
#include "postgres_fe.h"
#endif

#include "cluster/cluster_catalog_manifest.h"
#include "common/cryptohash.h"

static bool
manifest_overlap(const void *a, size_t alen, const void *b, size_t blen)
{
	uintptr_t ap = (uintptr_t)a;
	uintptr_t bp = (uintptr_t)b;

	if (a == NULL || b == NULL || alen == 0 || blen == 0)
		return false;
	return ap <= bp ? bp - ap < alen : ap - bp < blen;
}

static bool
manifest_uuid(const uint8 uuid[16], char hex[33])
{
	static const char digits[] = "0123456789abcdef";
	uint8 any = 0;

	for (unsigned i = 0; i < 16; ++i) {
		any |= uuid[i];
		hex[2 * i] = digits[uuid[i] >> 4];
		hex[2 * i + 1] = digits[uuid[i] & 15];
	}
	hex[32] = '\0';
	return any != 0;
}

bool
cluster_catalog_manifest_decode_initial(const uint8 *bytes, size_t length,
										const ClusterCatalogManifestIdentity *expected_identity,
										uint64 expected_generation, const uint8 expected_sha256[32],
										ClusterCatalogManifestInitial *out)
{
	char canonical[CLUSTER_CATALOG_MANIFEST_MAX_BYTES];
	char authority[33];
	char storage[33];
	uint8 digest[32];
	pg_cryptohash_ctx *ctx;
	bool hashed;
	int n;

	if (out == NULL || manifest_overlap(out, sizeof(*out), bytes, length)
		|| manifest_overlap(out, sizeof(*out), expected_identity, sizeof(*expected_identity))
		|| manifest_overlap(out, sizeof(*out), expected_sha256, sizeof(digest)))
		return false;
	memset(out, 0, sizeof(*out));
	if (bytes == NULL || expected_identity == NULL || expected_sha256 == NULL || length == 0
		|| length > sizeof(canonical) || expected_generation != 1
		|| expected_identity->system_identifier == 0 || expected_identity->database_incarnation == 0
		|| !manifest_uuid(expected_identity->authority_uuid, authority)
		|| !manifest_uuid(expected_identity->storage_uuid, storage))
		return false;

	/* The supported schema has exactly one spelling for each selected identity. */
	n = snprintf(canonical, sizeof(canonical),
				 "{\"database_identity\":{\"authority_uuid\":\"%s\",\"database_incarnation\":"
				 "\"" UINT64_FORMAT
				 "\",\"storage_uuid\":\"%s\",\"system_identifier\":\"" UINT64_FORMAT
				 "\"},\"entries\":[],\"generation\":\"1\",\"version\":1}\n",
				 authority, expected_identity->database_incarnation, storage,
				 expected_identity->system_identifier);
	if (n < 0 || (size_t)n >= sizeof(canonical) || (size_t)n != length
		|| memcmp(bytes, canonical, length) != 0)
		return false;

	ctx = pg_cryptohash_create(PG_SHA256);
	if (ctx == NULL)
		return false;
	hashed = pg_cryptohash_init(ctx) >= 0 && pg_cryptohash_update(ctx, bytes, length) >= 0
			 && pg_cryptohash_final(ctx, digest, sizeof(digest)) >= 0;
	pg_cryptohash_free(ctx);
	if (!hashed || memcmp(digest, expected_sha256, sizeof(digest)) != 0)
		return false;

	out->identity = *expected_identity;
	out->generation = 1;
	out->version = 1;
	out->entry_count = 0;
	return true;
}
