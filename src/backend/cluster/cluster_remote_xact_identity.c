/*-------------------------------------------------------------------------
 * cluster_remote_xact_identity.c
 *    Canonical identity digest for RF-SIDE v2 projections.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "common/cryptohash.h"
#include "common/sha2.h"

#include "access/xact.h"

#include "cluster/cluster_remote_xact.h"

#ifdef USE_PGRAC_CLUSTER

bool
cluster_remote_xact_prepare_digest_v2(uint64 system_identifier, int origin_node, TransactionId xid,
									  Oid database, const char *gid,
									  uint8 digest[CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES])
{
	static const uint8 domain[] = "PGRAC-RF-SIDE-PREPARE-V2";
	uint8 identity[20];
	uint8 sha256[PG_SHA256_DIGEST_LENGTH];
	size_t gid_len;
	pg_cryptohash_ctx *context;
	bool ok;
	int i;

	if (system_identifier == 0 || origin_node < 0 || origin_node >= (1 << 7)
		|| !TransactionIdIsNormal(xid) || !OidIsValid(database) || gid == NULL || digest == NULL)
		return false;
	gid_len = strnlen(gid, GIDSIZE);
	if (gid_len == 0 || gid_len >= GIDSIZE)
		return false;

	memset(identity, 0, sizeof(identity));
	for (i = 0; i < 8; i++)
		identity[i] = (uint8)(system_identifier >> (56 - i * 8));
	identity[8] = (uint8)origin_node;
	identity[9] = CLUSTER_REMOTE_XACT_ENTRY_FORMAT_V2;
	identity[10] = (uint8)((uint32)xid >> 24);
	identity[11] = (uint8)((uint32)xid >> 16);
	identity[12] = (uint8)((uint32)xid >> 8);
	identity[13] = (uint8)xid;
	identity[14] = (uint8)((uint32)database >> 24);
	identity[15] = (uint8)((uint32)database >> 16);
	identity[16] = (uint8)((uint32)database >> 8);
	identity[17] = (uint8)database;
	identity[18] = (uint8)(gid_len >> 8);
	identity[19] = (uint8)gid_len;

	context = pg_cryptohash_create(PG_SHA256);
	if (context == NULL)
		return false;
	ok = pg_cryptohash_init(context) >= 0
		 && pg_cryptohash_update(context, domain, sizeof(domain) - 1) >= 0
		 && pg_cryptohash_update(context, identity, sizeof(identity)) >= 0
		 && pg_cryptohash_update(context, (const uint8 *)gid, gid_len) >= 0
		 && pg_cryptohash_final(context, sha256, sizeof(sha256)) >= 0;
	pg_cryptohash_free(context);
	if (ok)
		memcpy(digest, sha256, CLUSTER_REMOTE_XACT_PREPARE_DIGEST_BYTES);
	return ok;
}

#endif
