/* Original common control and explicit empty catalog checkpoint manifest.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "cluster/cluster_cf_authority.h"
#include "common/cryptohash.h"
#include "cluster_initdb_common_private.h"

static bool
common_overlap(const void *left, Size length, const void *right, Size other)
{
	uintptr_t a = (uintptr_t)left, b = (uintptr_t)right;
	return left != NULL && right != NULL && (a <= b ? b - a < length : a - b < other);
}

static bool
common_hash(const uint8 *bytes, Size length, uint8 hash[32])
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);
	bool ok;
	if (ctx == NULL)
		return false;
	ok = pg_cryptohash_init(ctx) >= 0 && pg_cryptohash_update(ctx, bytes, length) >= 0
		 && pg_cryptohash_final(ctx, hash, 32) >= 0;
	pg_cryptohash_free(ctx);
	return ok;
}

static bool
common_equal(const ControlFileData *a, const ControlFileData *b)
{
#define SAME(field)                                                                                \
	if (a->field != b->field)                                                                      \
	return false
	SAME(system_identifier);
	SAME(pg_control_version);
	SAME(catalog_version_no);
	if (!FullTransactionIdEquals(a->checkPointCopy.nextXid, b->checkPointCopy.nextXid))
		return false;
	SAME(checkPointCopy.nextOid);
	SAME(checkPointCopy.nextMulti);
	SAME(checkPointCopy.nextMultiOffset);
	SAME(checkPointCopy.oldestXid);
	SAME(checkPointCopy.oldestXidDB);
	SAME(checkPointCopy.oldestActiveXid);
	SAME(checkPointCopy.oldestMulti);
	SAME(checkPointCopy.oldestMultiDB);
	SAME(wal_level);
	SAME(wal_log_hints);
	SAME(MaxConnections);
	SAME(max_worker_processes);
	SAME(max_wal_senders);
	SAME(max_prepared_xacts);
	SAME(max_locks_per_xact);
	SAME(track_commit_timestamp);
	SAME(maxAlign);
	SAME(floatFormat);
	SAME(blcksz);
	SAME(relseg_size);
	SAME(xlog_blcksz);
	SAME(xlog_seg_size);
	SAME(nameDataLen);
	SAME(indexMaxKeys);
	SAME(toast_max_chunk_size);
	SAME(loblksize);
	SAME(float8ByVal);
	SAME(data_checksum_version);
#undef SAME
	return true;
}

bool
cluster_initdb_common_build(const ClusterSharedConfigRef *config,
							const ControlFileData *const sources[CLUSTER_CONTROL_ROOT_RECORD_COUNT],
							ClusterInitdbCommon *out)
{
	ClusterInitdbCommon result = { 0 };
	const ControlFileData *founder;
	char authority[33], storage[33];
	const uint8 zero[16] = { 0 };
	int length;

	if (out == NULL)
		return false;
	if (common_overlap(config, sizeof(*config), out, sizeof(*out))
		|| common_overlap(sources, sizeof(*sources) * CLUSTER_CONTROL_ROOT_RECORD_COUNT, out,
						  sizeof(*out)))
		return false;
	if (sources != NULL)
		for (unsigned node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++)
			if (common_overlap(sources[node], sizeof(ControlFileData), out, sizeof(*out)))
				return false;
	memset(out, 0, sizeof(*out));
	if (config == NULL || sources == NULL || !(config->identity.configured[0] & 1)
		|| config->identity.generation != 1 || config->identity.database_incarnation == 0
		|| config->identity.system_identifier == 0 || sources[0] == NULL
		|| memcmp(config->identity.authority_uuid, zero, 16) == 0
		|| memcmp(config->identity.storage_uuid, zero, 16) == 0)
		return false;
	founder = sources[0];
	for (unsigned node = 0; node < CLUSTER_CONTROL_ROOT_RECORD_COUNT; node++) {
		const ControlFileData *cf = sources[node];
		bool configured
			= (config->identity.configured[node / 64] & (UINT64CONST(1) << (node % 64))) != 0;
		if (configured != (cf != NULL))
			return false;
		if (!configured)
			continue;
		if (cluster_cf_classify_buffer((const char *)cf, sizeof(*cf),
									   config->identity.system_identifier)
				!= CLUSTER_CF_VALID
			|| cf->state != DB_SHUTDOWNED || cf->checkPoint == InvalidXLogRecPtr
			|| cf->checkPointCopy.redo != cf->checkPoint || cf->checkPointCopy.ThisTimeLineID != 1
			|| cf->checkPointCopy.PrevTimeLineID != 1 || cf->minRecoveryPoint != 0
			|| cf->minRecoveryPointTLI != 0 || cf->backupStartPoint != 0 || cf->backupEndPoint != 0
			|| cf->backupEndRequired || cf->track_commit_timestamp || cf->max_prepared_xacts != 0
			|| !common_equal(cf, founder))
			return false;
	}
	if (cluster_cf_control_image_encode(founder, result.control) != CLUSTER_CONTROL_ROOT_OK_PRIMARY
		|| !common_hash(result.control, sizeof(result.control), result.control_sha256))
		return false;
	for (unsigned i = 0; i < 16; i++) {
		snprintf(authority + i * 2, 3, "%02x", config->identity.authority_uuid[i]);
		snprintf(storage + i * 2, 3, "%02x", config->identity.storage_uuid[i]);
	}
	/* Fixed canonical representation of the original empty publication set.
	 * uint64s are decimal strings; this is not an online DDL coverage claim. */
	length = snprintf((char *)result.catalog, sizeof(result.catalog),
					  "{\"database_identity\":{\"authority_uuid\":\"%s\",\"database_incarnation\":"
					  "\"" UINT64_FORMAT
					  "\",\"storage_uuid\":\"%s\",\"system_identifier\":\"" UINT64_FORMAT
					  "\"},\"entries\":[],\"generation\":\"1\",\"version\":1}\n",
					  authority, config->identity.database_incarnation, storage,
					  config->identity.system_identifier);
	if (length <= 0 || length >= sizeof(result.catalog)
		|| !common_hash(result.catalog, length, result.catalog_sha256))
		return false;
	result.catalog_length = length;
	*out = result;
	return true;
}
