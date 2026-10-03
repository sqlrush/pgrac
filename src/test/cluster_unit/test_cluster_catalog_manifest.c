/* Exercise original producer bytes through the strict manifest consumer.
 * Author: SqlRush <sqlrush@gmail.com> */
#include "postgres.h"
#include "catalog/catversion.h"
#include "cluster/cluster_catalog_manifest.h"
#include "cluster/cluster_cf_authority.h"
#include "common/cryptohash.h"
#include "storage/bufpage.h"
#include "unit_test.h"
#include "../../backend/cluster/cluster_initdb_common_private.h"

UT_DEFINE_GLOBALS();

static ClusterInitdbCommon publication;
static ClusterCatalogManifestIdentity identity;
static ClusterCatalogManifestInitial decoded;
static uint8 bytes[1024];
static uint8 hash[32];
static size_t length;

void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	abort();
}

static void
digest(void)
{
	pg_cryptohash_ctx *ctx = pg_cryptohash_create(PG_SHA256);
	UT_ASSERT(ctx != NULL);
	UT_ASSERT(pg_cryptohash_init(ctx) >= 0);
	UT_ASSERT(pg_cryptohash_update(ctx, bytes, length) >= 0);
	UT_ASSERT(pg_cryptohash_final(ctx, hash, sizeof(hash)) >= 0);
	pg_cryptohash_free(ctx);
}

static void
prepare(uint64 sysid, uint64 incarnation)
{
	ClusterSharedConfigRef config = { 0 };
	ControlFileData cf = { 0 };
	const ControlFileData *sources[CLUSTER_CONTROL_ROOT_RECORD_COUNT] = { 0 };
	config.identity.system_identifier = sysid;
	config.identity.database_incarnation = incarnation;
	config.identity.generation = 1;
	config.identity.configured[0] = 15;
	memset(config.identity.authority_uuid, 0xab, 16);
	memset(config.identity.storage_uuid, 0xcd, 16);
	cf.system_identifier = sysid;
	cf.pg_control_version = PG_CONTROL_VERSION;
	cf.catalog_version_no = CATALOG_VERSION_NO;
	cf.state = DB_SHUTDOWNED;
	cf.checkPoint = cf.checkPointCopy.redo = 0x2000028;
	cf.checkPointCopy.ThisTimeLineID = cf.checkPointCopy.PrevTimeLineID = 1;
	cf.checkPointCopy.nextXid = FullTransactionIdFromU64(750);
	cf.checkPointCopy.nextOid = 16000;
	cf.checkPointCopy.oldestXid = 3;
	cf.checkPointCopy.oldestXidDB = 1;
	cf.checkPointCopy.nextMulti = cf.checkPointCopy.oldestMulti = 1;
	cf.checkPointCopy.oldestMultiDB = 1;
	cf.blcksz = BLCKSZ;
	cf.data_checksum_version = PG_DATA_CHECKSUM_VERSION;
	INIT_CRC32C(cf.crc);
	COMP_CRC32C(cf.crc, &cf, offsetof(ControlFileData, crc));
	FIN_CRC32C(cf.crc);
	for (unsigned i = 0; i < 4; ++i)
		sources[i] = &cf;
	UT_ASSERT(cluster_initdb_common_build(&config, sources, &publication));
	memcpy(identity.authority_uuid, config.identity.authority_uuid, 16);
	memcpy(identity.storage_uuid, config.identity.storage_uuid, 16);
	identity.database_incarnation = incarnation;
	identity.system_identifier = sysid;
	length = publication.catalog_length;
	memset(bytes, 0, sizeof(bytes));
	memcpy(bytes, publication.catalog, length);
	memcpy(hash, publication.catalog_sha256, sizeof(hash));
}

static void
reset(void)
{
	prepare(UINT64CONST(7584383251700000001), 1);
}

static bool
decode(void)
{
	return cluster_catalog_manifest_decode_initial(bytes, length, &identity, 1, hash, &decoded);
}

static void
reject(void)
{
	ClusterCatalogManifestInitial zero = { 0 };
	memset(&decoded, 0xa5, sizeof(decoded));
	UT_ASSERT(!decode());
	UT_ASSERT(memcmp(&decoded, &zero, sizeof(decoded)) == 0);
}

static void
replace(const char *from, const char *to)
{
	char *at = strstr((char *)bytes, from);
	size_t offset, old = strlen(from), added = strlen(to);
	UT_ASSERT(at != NULL);
	if (at == NULL)
		return;
	offset = at - (char *)bytes;
	UT_ASSERT(length - old + added < sizeof(bytes));
	memmove(at + added, at + old, length - offset - old + 1);
	memcpy(at, to, added);
	length = length - old + added;
	digest(); /* malformed input must fail even when its supplied hash matches */
}

UT_TEST(original_producer_roundtrip)
{
	reset();
	UT_ASSERT(decode());
	UT_ASSERT(memcmp(&decoded.identity, &identity, sizeof(identity)) == 0);
	UT_ASSERT_EQ(decoded.generation, 1);
	UT_ASSERT_EQ(decoded.version, 1);
	UT_ASSERT_EQ(decoded.entry_count, 0);
}
UT_TEST(maximum_uint64_is_not_truncated)
{
	prepare(UINT64_MAX, UINT64_MAX);
	UT_ASSERT(decode());
	UT_ASSERT_EQ(decoded.identity.system_identifier, UINT64_MAX);
	UT_ASSERT_EQ(decoded.identity.database_incarnation, UINT64_MAX);
}
UT_TEST(namespace_is_exact)
{
	reset();
	identity.authority_uuid[0] ^= 1;
	reject();
	reset();
	identity.storage_uuid[0] ^= 1;
	reject();
	reset();
	identity.database_incarnation++;
	reject();
	reset();
	identity.system_identifier++;
	reject();
}
UT_TEST(generation_is_original_and_selected)
{
	reset();
	UT_ASSERT(
		!cluster_catalog_manifest_decode_initial(bytes, length, &identity, 2, hash, &decoded));
	replace("\"generation\":\"1\"", "\"generation\":\"2\"");
	reject();
	UT_ASSERT(
		!cluster_catalog_manifest_decode_initial(bytes, length, &identity, 2, hash, &decoded));
}
UT_TEST(hash_binds_final_lf)
{
	reset();
	hash[0] ^= 1;
	reject();
	reset();
	length--;
	digest();
	reject();
}
UT_TEST(unknown_or_duplicate_keys)
{
	reset();
	replace("\"version\":1", "\"unknown\":0,\"version\":1");
	reject();
	reset();
	replace("\"version\":1", "\"version\":1,\"version\":1");
	reject();
	reset();
	replace("\"database_incarnation\":\"1\"",
			"\"database_incarnation\":\"1\",\"database_incarnation\":\"1\"");
	reject();
}
UT_TEST(key_order_and_whitespace)
{
	reset();
	replace("\"entries\":[],\"generation\":\"1\"", "\"generation\":\"1\",\"entries\":[]");
	reject();
	reset();
	replace("\"entries\":[]", "\"entries\": []");
	reject();
	reset();
	replace("}\n", "}\r\n");
	reject();
}
UT_TEST(canonical_uint64_strings)
{
	const char *invalid[]
		= { "\"01\"", "\"+1\"", "\"0\"", "\"-1\"", "\"1.0\"", "1", "\"18446744073709551616\"" };
	for (unsigned i = 0; i < lengthof(invalid); ++i) {
		char field[96];
		reset();
		snprintf(field, sizeof(field), "\"database_incarnation\":%s", invalid[i]);
		replace("\"database_incarnation\":\"1\"", field);
		reject();
	}
	reset();
	replace("\"generation\":\"1\"", "\"generation\":\"01\"");
	reject();
}
UT_TEST(canonical_uuid_spelling)
{
	reset();
	replace("abab", "Abab");
	reject();
	reset();
	replace("abab", "ab-ab");
	reject();
	reset();
	replace("abab", "\\u0061bab");
	reject();
}
UT_TEST(only_explicit_empty_entries)
{
	const char *invalid[] = { "null", "{}", "[{}]", "[0]", "[ ]" };
	for (unsigned i = 0; i < lengthof(invalid); ++i) {
		char field[64];
		reset();
		snprintf(field, sizeof(field), "\"entries\":%s", invalid[i]);
		replace("\"entries\":[]", field);
		reject();
	}
}
UT_TEST(version_is_numeric_one)
{
	const char *invalid[] = { "\"1\"", "2", "0", "1.0", "1e0" };
	for (unsigned i = 0; i < lengthof(invalid); ++i) {
		char field[64];
		reset();
		snprintf(field, sizeof(field), "\"version\":%s", invalid[i]);
		replace("\"version\":1", field);
		reject();
	}
}
UT_TEST(every_truncated_prefix)
{
	size_t full;
	reset();
	full = length;
	for (length = 0; length < full; ++length) {
		digest();
		reject();
	}
}
UT_TEST(trailing_bytes_and_embedded_nul)
{
	reset();
	bytes[length++] = '\n';
	digest();
	reject();
	reset();
	bytes[length++] = 0;
	digest();
	reject();
	reset();
	bytes[length / 2] = 0;
	digest();
	reject();
}
UT_TEST(invalid_expected_namespace)
{
	reset();
	memset(identity.authority_uuid, 0, 16);
	reject();
	reset();
	memset(identity.storage_uuid, 0, 16);
	reject();
	reset();
	identity.system_identifier = 0;
	reject();
	reset();
	identity.database_incarnation = 0;
	reject();
}
UT_TEST(null_and_oversize_inputs)
{
	reset();
	UT_ASSERT(!cluster_catalog_manifest_decode_initial(NULL, length, &identity, 1, hash, &decoded));
	UT_ASSERT(!cluster_catalog_manifest_decode_initial(bytes, length, NULL, 1, hash, &decoded));
	UT_ASSERT(
		!cluster_catalog_manifest_decode_initial(bytes, length, &identity, 1, NULL, &decoded));
	UT_ASSERT(!cluster_catalog_manifest_decode_initial(bytes, length, &identity, 1, hash, NULL));
	length = CLUSTER_CATALOG_MANIFEST_MAX_BYTES + 1;
	digest();
	reject();
}
UT_TEST(overlap_never_changes_inputs)
{
	uint8 saved[sizeof(bytes)];
	ClusterCatalogManifestIdentity identity_copy;
	reset();
	memcpy(saved, bytes, sizeof(bytes));
	identity_copy = identity;
	UT_ASSERT(!cluster_catalog_manifest_decode_initial(bytes, length, &identity, 1, hash,
													   (ClusterCatalogManifestInitial *)bytes));
	UT_ASSERT(memcmp(saved, bytes, sizeof(bytes)) == 0);
	UT_ASSERT(!cluster_catalog_manifest_decode_initial(bytes, length, &identity, 1, hash,
													   (ClusterCatalogManifestInitial *)&identity));
	UT_ASSERT(memcmp(&identity_copy, &identity, sizeof(identity)) == 0);
}
UT_TEST(input_bytes_are_immutable)
{
	uint8 saved[sizeof(bytes)];
	reset();
	memcpy(saved, bytes, sizeof(bytes));
	UT_ASSERT(decode());
	UT_ASSERT(memcmp(saved, bytes, sizeof(bytes)) == 0);
	hash[0] ^= 1;
	reject();
	UT_ASSERT(memcmp(saved, bytes, sizeof(bytes)) == 0);
}

int
main(void)
{
	UT_PLAN(17);
	UT_RUN(original_producer_roundtrip);
	UT_RUN(maximum_uint64_is_not_truncated);
	UT_RUN(namespace_is_exact);
	UT_RUN(generation_is_original_and_selected);
	UT_RUN(hash_binds_final_lf);
	UT_RUN(unknown_or_duplicate_keys);
	UT_RUN(key_order_and_whitespace);
	UT_RUN(canonical_uint64_strings);
	UT_RUN(canonical_uuid_spelling);
	UT_RUN(only_explicit_empty_entries);
	UT_RUN(version_is_numeric_one);
	UT_RUN(every_truncated_prefix);
	UT_RUN(trailing_bytes_and_embedded_nul);
	UT_RUN(invalid_expected_namespace);
	UT_RUN(null_and_oversize_inputs);
	UT_RUN(overlap_never_changes_inputs);
	UT_RUN(input_bytes_are_immutable);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
